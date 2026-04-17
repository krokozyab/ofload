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
 * Groups are a hint for parallelism: targets sharing a group id are executed in
 * parallel on the shared [etlPool]. They do NOT share a source connection — every
 * target opens its own source connection via [OwnedSourceConn] so there is no way
 * for concurrent workers to step on each other's JDBC state. Lone targets (no group
 * or group size 1) run synchronously on the caller thread.
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
     * Per-target source connection wrapper with safe reconnect on auth/session failure.
     *
     * Each target owns its own instance — connections are NOT shared across targets or
     * threads. That removes the thread-safety burden on the underlying JDBC driver
     * (ofjdbc is a REST wrapper and not guaranteed thread-safe) and sidesteps races
     * between `reconnect()` and in-flight queries on other workers. The per-target
     * isolation costs one SSO handshake per target but buys correctness.
     */
    private class OwnedSourceConn : AutoCloseable {
        private val log = LoggerFactory.getLogger(OwnedSourceConn::class.java)
        var conn: Connection = SourceDs.getConnection()
            private set

        /** Close the current connection and open a fresh one. Called on auth-failure retry. */
        fun reconnect() {
            log.warn("Reconnecting to source (SSO refresh)...")
            runCatching { conn.close() }
            conn = SourceDs.getConnection()
            log.info("Source reconnected")
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
     * group, each target gets its own source connection and executes in parallel on
     * [etlPool]. This bounds cross-target interference to zero at the cost of one SSO
     * login per target per run.
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
                log.info("Group {}: running {} in parallel (each with its own source connection)",
                    groupId, grpTargets.map { it.name })
                val futures = grpTargets.map { target ->
                    etlPool.submit<Unit> {
                        MDC.put("group", groupId.toString())
                        runTarget(target)
                        MDC.remove("group")
                    }
                }
                // Block until every parallel worker finishes.
                futures.forEach { it.get() }
                log.info("Group {}: done", groupId)
                MDC.remove("group")
            }
        }
    }

    /**
     * Full lifecycle of a single target on a single worker.
     *
     * Steps: open ATP connection (autoCommit=false, we control txn boundaries); open
     * a dedicated source connection via [OwnedSourceConn]; read last watermark and
     * mark run as RUNNING; loop over pages of streaming load + merge + watermark
     * update until the source returns less than pageSize rows; mark run as OK.
     * On any failure (including JVM Error like OutOfMemoryError), roll back, record
     * FAILED on a fresh connection, log — then re-throw Errors so the JVM can
     * terminate cleanly instead of silently continuing in a broken state.
     */
    private fun runTarget(target: TargetConfig) {
        val t = target.name
        MDC.put("target", t)
        log.info("[{}] Starting", t)
        OracleDs.getConnection().use { oracleConn ->
            log.info("[{}] ATP connected", t)
            oracleConn.autoCommit = false
            OwnedSourceConn().use { source ->
                log.info("[{}] Source connected", t)
                try {
                    var lastWm = WatermarkStore.read(oracleConn, t)
                    WatermarkStore.beginRun(oracleConn, t, target.watermarkColumn)
                    oracleConn.commit()

                    var totalLoaded = 0L
                    var totalMerged = 0L
                    var pageNum = 0
                    var lastKeyValues: List<String?>? = null

                    // Page loop: one iteration per staging+merge cycle. Terminates when
                    // the source returns fewer rows than pageSize or when page-level rows
                    // hit zero. Each page's watermark is committed immediately, so a crash
                    // on page N leaves pages 0..N-1 durable.
                    do {
                        pageNum++
                        MDC.put("page", pageNum.toString())

                        val result = loadPageWithRetry(target, source, oracleConn, lastWm, lastKeyValues, pageNum)

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
                    // JVM-level Errors still produce a FAILED row — otherwise the run
                    // would be left RUNNING forever and only resetStaleRunning at next
                    // startup would clean it up. Record the failure on a FRESH connection
                    // because oracleConn may already be poisoned.
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
        }
        MDC.remove("target")
        MDC.remove("page")
    }

    /**
     * Wrap [StagingLoader.load] with a retry loop for transient source-auth failures.
     *
     * If the source throws an auth-like error (401, token expired, session invalidated)
     * we reconnect via the target's own [OwnedSourceConn] and retry up to
     * [MAX_SOURCE_RETRIES] times — staging is truncated on entry so retries are
     * idempotent. Any other exception propagates immediately.
     */
    private fun loadPageWithRetry(
        target: TargetConfig,
        source: OwnedSourceConn,
        oracleConn: Connection,
        lastWm: String?,
        lastKeyValues: List<String?>?,
        pageNum: Int
    ): LoadResult {
        val t = target.name
        var lastError: Exception? = null

        for (attempt in 1..MAX_SOURCE_RETRIES + 1) {
            try {
                log.info("[{}] Loading page {} (wm={}, lastKeys={})", t, pageNum, lastWm, lastKeyValues)
                return StagingLoader.load(target, source.conn, oracleConn, lastWm, lastKeyValues)
            } catch (e: Exception) {
                lastError = e
                if (attempt <= MAX_SOURCE_RETRIES && isSourceAuthError(e)) {
                    log.warn("[{}] Source error on attempt {}/{}: {}. Reconnecting...",
                        t, attempt, MAX_SOURCE_RETRIES + 1, e.message)
                    source.reconnect()
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
