package app.etl

import app.config.PipelineConfig
import app.config.TargetConfig
import app.db.OracleDs
import app.db.SourceDs
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.sql.Connection
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * ETL orchestrator — walks the list of targets, grouping them, and drives each target
 * through the full lifecycle: watermark read → source stream to staging → merge into
 * final table → watermark update.
 *
 * Groups are the unit of source-side parallelism and SSO amortisation: targets sharing
 * a group id open ONE shared Fusion connection (one SSO handshake) and then run their
 * pages in parallel over it. Lone targets (no group or group size 1) open their own
 * connection. The ATP side uses the UCP pool in either case — no target is blocked
 * by another on the target side.
 *
 * Idempotency: every run truncates staging and re-applies MERGE, so restarting a
 * partially-complete run cannot produce duplicates. Watermarks are persisted after
 * each successful page, so progress survives crashes.
 */
class Pipeline(private val cfg: PipelineConfig) {
    private val log = LoggerFactory.getLogger(Pipeline::class.java)

    companion object {
        /** How many times to retry a source auth/session failure before giving up. */
        private const val MAX_SOURCE_RETRIES = 2
    }

    /**
     * Long-lived worker pool shared by every group's parallel target execution.
     *
     * Using a single cachedThreadPool (instead of per-group newFixedThreadPool) avoids
     * the overhead of creating and shutting down an executor for each group and avoids
     * the nested-executors antipattern where RunManager's pool runs a task that
     * creates its own pool. Threads are daemon so they don't block JVM exit if we
     * forget to shut the pool down; [shutdown] still awaits cleanly on the normal path.
     */
    private val etlPool = Executors.newCachedThreadPool { r ->
        Thread(r, "etl-worker").apply { isDaemon = true }
    }

    /**
     * A rebindable source connection shared between targets of the same group.
     *
     * Fusion SSO sessions can expire or drop mid-run; when that happens we want to
     * transparently re-open the underlying connection instead of failing the whole
     * group. [reconnect] is synchronized so only one thread triggers the (expensive)
     * SSO handshake when several parallel workers notice the drop simultaneously.
     */
    class SourceConnHolder(initial: Connection) : AutoCloseable {
        @Volatile var conn: Connection = initial
            private set

        /**
         * Close the current connection and open a fresh one via [SourceDs.getConnection].
         * Synchronized so concurrent parallel workers cannot trigger duplicate SSO logins.
         */
        @Synchronized
        fun reconnect(): Connection {
            val log = LoggerFactory.getLogger(SourceConnHolder::class.java)
            log.warn("Reconnecting to source (SSO refresh)...")
            runCatching { conn.close() }
            conn = SourceDs.getConnection()
            log.info("Source reconnected")
            return conn
        }

        override fun close() {
            runCatching { conn.close() }
        }
    }

    /** Run every target in the loaded config. Used by Main for full-pipeline batch runs. */
    fun run() = runTargets(cfg.targets)

    /**
     * Gracefully stop the shared ETL worker pool.
     *
     * Called from [app.server.RunManager.shutdown] after the RunManager's submission
     * pool has drained — at that point no new targets can be queued, so awaiting the
     * etlPool here is bounded by whatever target work is already in flight.
     * Waits up to 5 minutes, then force-cancels.
     */
    fun shutdown() {
        log.info("Pipeline etlPool shutting down...")
        etlPool.shutdown()
        if (!etlPool.awaitTermination(5, TimeUnit.MINUTES)) {
            log.warn("Pipeline etlPool did not terminate cleanly, forcing shutdown")
            etlPool.shutdownNow()
        }
    }

    /**
     * Run the given subset of targets, respecting group membership.
     *
     * Targets are partitioned by group id (null groups become unique singletons) and
     * groups are run sequentially in ascending group-id order. Within a multi-target
     * group, all targets share one [SourceConnHolder] and execute in parallel on a
     * per-group thread pool sized to the group's target count. This amortises source-
     * side SSO cost while bounding parallelism to something ATP and Fusion can handle.
     */
    fun runTargets(targets: List<TargetConfig>) {
        // Ungrouped targets get synthetic decreasing ids so they each form a single-
        // element group, and ordered groups come first (ascending) — a deterministic
        // execution order helpful for reproducing issues.
        var seqId = Int.MIN_VALUE
        val groups = targets
            .map { it to (it.group ?: seqId--) }
            .groupBy({ it.second }, { it.first })
            .toSortedMap()

        for ((groupId, grpTargets) in groups) {
            if (grpTargets.size == 1) {
                runTarget(grpTargets[0])
            } else {
                MDC.put("group", groupId.toString())
                log.info("Group {}: running {} in parallel", groupId, grpTargets.map { it.name })
                log.info("Group {}: connecting to source (single SSO)", groupId)
                SourceConnHolder(SourceDs.getConnection()).use { holder ->
                    log.info("Group {}: source connected", groupId)
                    // Submit to the shared etlPool rather than creating a per-group pool.
                    val futures = grpTargets.map { target ->
                        etlPool.submit<Unit> {
                            MDC.put("group", groupId.toString())
                            runTarget(target, holder)
                            MDC.remove("group")
                        }
                    }
                    // Block until every parallel worker finishes so the group's
                    // shared connection isn't closed while someone is still using it.
                    futures.forEach { it.get() }
                }
                log.info("Group {}: done", groupId)
                MDC.remove("group")
            }
        }
    }

    /**
     * Full lifecycle of a single target on a single worker.
     *
     * Steps: open ATP connection (autoCommit=false, we control txn boundaries);
     * read last watermark and mark run as RUNNING; loop over pages of streaming
     * load + merge + watermark update until the source returns less than pageSize
     * rows; mark run as OK. On any failure (including JVM Error like OutOfMemoryError),
     * roll back, record FAILED on a fresh connection, log — then re-throw Errors so
     * the JVM can terminate cleanly instead of silently continuing in a broken state.
     */
    private fun runTarget(target: TargetConfig, sourceHolder: SourceConnHolder? = null) {
        val t = target.name
        MDC.put("target", t)
        log.info("[{}] Starting", t)
        OracleDs.getConnection().use { oracleConn ->
            log.info("[{}] ATP connected", t)
            oracleConn.autoCommit = false
            try {
                var lastWm = WatermarkStore.read(oracleConn, t)
                WatermarkStore.beginRun(oracleConn, t, target.watermarkColumn)
                oracleConn.commit()

                var totalLoaded = 0L
                var totalMerged = 0L
                var pageNum = 0
                var lastKeyValues: List<String?>? = null

                // Page loop: one iteration per staging+merge cycle. Terminates when the
                // source returns fewer rows than pageSize (no more pages) or when page-
                // level rows hit zero. Each page's watermark is committed immediately,
                // so a crash on page N leaves pages 0..N-1 durable.
                do {
                    pageNum++
                    MDC.put("page", pageNum.toString())

                    val result = loadPageWithRetry(target, sourceHolder, oracleConn, lastWm, lastKeyValues, pageNum)

                    if (result.rowsLoaded == 0L) break

                    val startMerge = System.currentTimeMillis()
                    val merged = TargetMerger.merge(oracleConn, target)
                    log.info("[{}] Merged {} rows in {}ms", t, merged, System.currentTimeMillis() - startMerge)
                    totalLoaded += result.rowsLoaded
                    totalMerged += merged
                    lastWm = result.newWm
                    lastKeyValues = result.lastKeyValues

                    WatermarkStore.finishRun(oracleConn, t, lastWm, totalLoaded)
                    oracleConn.commit()

                    if (target.pageSize != null) {
                        log.info("[{}] Page {}: loaded={}, merged={} | Total: loaded={}, merged={} | wm={}",
                            t, pageNum, result.rowsLoaded, merged, totalLoaded, totalMerged, lastWm)
                    }
                } while (target.pageSize != null && result.rowsLoaded.toInt() == target.pageSize)

                WatermarkStore.finishRun(oracleConn, t, lastWm, totalLoaded)
                oracleConn.commit()
                log.info("[{}] OK: total_loaded={}, total_merged={}, new_wm={}", t, totalLoaded, totalMerged, lastWm)
            } catch (e: Throwable) {
                // Catch Throwable (not just Exception) so OutOfMemoryError and other
                // JVM-level Errors still produce a FAILED row — otherwise the run would
                // be left RUNNING forever and only resetStaleRunning at next startup
                // would clean it up. Record the failure on a FRESH connection because
                // oracleConn may already be poisoned.
                runCatching { oracleConn.rollback() }
                runCatching {
                    OracleDs.getConnection().use { failConn ->
                        failConn.autoCommit = true
                        WatermarkStore.failRun(failConn, t, e.message ?: "unknown error")
                    }
                }.onFailure { log.warn("[{}] Failed to record FAILED status: {}", t, it.message) }
                log.error("[{}] FAILED: {}", t, e.message, e)
                // Re-throw Errors so the JVM doesn't silently continue in a broken
                // state (OOM, StackOverflow, etc). Plain Exceptions are swallowed —
                // the FAILED row in ETL_WATERMARK is the caller-visible signal.
                if (e is Error) throw e
            }
        }
        MDC.remove("target")
        MDC.remove("page")
    }

    /**
     * Wrap [StagingLoader.load] with a retry loop for transient source-auth failures.
     *
     * If the source throws an auth-like error (401, token expired, session invalidated)
     * we reconnect via the shared holder and retry up to [MAX_SOURCE_RETRIES] times —
     * staging is already truncated at that point, so retries are idempotent. Any other
     * exception propagates immediately. Non-group runs without a holder open a fresh
     * connection per attempt.
     */
    private fun loadPageWithRetry(
        target: TargetConfig,
        sourceHolder: SourceConnHolder?,
        oracleConn: Connection,
        lastWm: String?,
        lastKeyValues: List<String?>?,
        pageNum: Int
    ): LoadResult {
        val t = target.name
        var lastError: Exception? = null

        for (attempt in 1..MAX_SOURCE_RETRIES + 1) {
            try {
                return if (sourceHolder != null) {
                    log.info("[{}] Loading page {} (wm={}, lastKeys={})", t, pageNum, lastWm, lastKeyValues)
                    StagingLoader.load(target, sourceHolder.conn, oracleConn, lastWm, lastKeyValues)
                } else {
                    log.info("[{}] Connecting to source", t)
                    SourceDs.getConnection().use { srcConn ->
                        log.info("[{}] Source connected, loading page {} (wm={}, lastKeys={})", t, pageNum, lastWm, lastKeyValues)
                        StagingLoader.load(target, srcConn, oracleConn, lastWm, lastKeyValues)
                    }
                }
            } catch (e: Exception) {
                lastError = e
                if (attempt <= MAX_SOURCE_RETRIES && isSourceAuthError(e)) {
                    log.warn("[{}] Source error on attempt {}/{}: {}. Reconnecting...",
                        t, attempt, MAX_SOURCE_RETRIES + 1, e.message)
                    if (sourceHolder != null) {
                        sourceHolder.reconnect()
                    }
                    // Staging was truncated on entry, safe to retry without dedup concerns.
                } else {
                    throw e
                }
            }
        }
        throw lastError!!
    }

    /**
     * Heuristic classifier for "is this a transient SSO/auth failure worth retrying?".
     *
     * Matches on the exception message text — not ideal, but ofjdbc surfaces auth
     * failures as generic SQLExceptions without a dedicated vendor code, so string
     * matching is the only signal available. False positives only cost one extra
     * reconnect+retry, so the matcher is deliberately lenient.
     */
    private fun isSourceAuthError(e: Exception): Boolean {
        val msg = e.message?.lowercase() ?: ""
        return msg.contains("401") ||
               msg.contains("unauthorized") ||
               msg.contains("authentication") ||
               msg.contains("token") ||
               msg.contains("session") ||
               msg.contains("login")
    }
}
