package app.etl

import app.config.PipelineConfig
import app.config.TargetConfig
import app.db.OracleDs
import app.db.SourceDs
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.sql.Connection
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * ETL orchestrator — walks the list of targets, grouping them, and drives each target
 * through the full lifecycle: watermark read → source stream to staging → merge into
 * final table → watermark update.
 *
 * Groups are a hint for parallelism: targets sharing a group id can be scheduled in
 * parallel on the shared [etlPool]. All workers share ONE process-wide source
 * connection through [SourceDs.withConnection] (mutex-guarded) so only one SSO
 * browser window ever opens per process. Parallel workers therefore serialise on
 * the source-read phase but still parallelise ATP-side merge work. Lone targets
 * (no group or group size 1) run synchronously on the caller thread.
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
     * Bounded on both threads and queue to prevent runaway growth under load:
     *  - `ETL_WORKERS` (env, default 8) — maximum concurrent target workers
     *  - `ETL_QUEUE_SIZE` (env, default 32) — pending submissions before backpressure
     *
     * When the queue is full the CallerRunsPolicy takes over: the submitting thread
     * (a RunManager worker that is already running [runTargets]) executes the task
     * inline. That serialises excess work instead of creating unbounded threads and
     * keeps forward progress on legitimate load spikes. Daemon threads so the JVM
     * can exit if [shutdown] is skipped; [shutdown] still awaits cleanly on the
     * normal path.
     */
    private val etlPool = ThreadPoolExecutor(
        (System.getenv("ETL_WORKERS")?.toIntOrNull() ?: 8),
        (System.getenv("ETL_WORKERS")?.toIntOrNull() ?: 8),
        0L, TimeUnit.MILLISECONDS,
        ArrayBlockingQueue<Runnable>(System.getenv("ETL_QUEUE_SIZE")?.toIntOrNull() ?: 32),
        { r -> Thread(r, "etl-worker").apply { isDaemon = true } },
        ThreadPoolExecutor.CallerRunsPolicy()
    )

    /**
     * Run every target in the loaded config with a locally-generated runId.
     * For HTTP-driven runs use [runTargets] directly with the RunManager's runId so
     * the UI can correlate the run with what it submitted.
     */
    fun run() = runTargets(cfg.targets, UUID.randomUUID().toString())

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
    fun runTargets(targets: List<TargetConfig>, runId: String) {
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
                runTarget(grpTargets[0], runId)
            } else {
                MDC.put("group", groupId.toString())
                log.info("Group {}: scheduling {} in parallel (source reads serialise on shared connection)",
                    groupId, grpTargets.map { it.name })
                val futures = grpTargets.map { target ->
                    etlPool.submit<Unit> {
                        MDC.put("group", groupId.toString())
                        runTarget(target, runId)
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
     * Steps: open ATP connection (autoCommit=false, we control txn boundaries); read
     * last watermark and mark run as RUNNING; loop over pages of streaming load +
     * merge + watermark update until the source returns less than pageSize rows;
     * mark run as OK. The source connection is borrowed per-page from the shared
     * [SourceDs] singleton via [loadPageWithRetry] — the mutex is only held for the
     * duration of the source read, so other workers can merge/commit in parallel.
     *
     * On any failure (including JVM Error like OutOfMemoryError), roll back, record
     * FAILED on a fresh connection, log — then re-throw Errors so the JVM can
     * terminate cleanly instead of silently continuing in a broken state.
     */
    private fun runTarget(target: TargetConfig, runId: String) {
        val t = target.name
        MDC.put("target", t)
        MDC.put("runId", runId)
        log.info("[{}] Starting (runId={})", t, runId)
        OracleDs.getConnection().use { oracleConn ->
            log.info("[{}] ATP connected", t)
            oracleConn.autoCommit = false
            try {
                val runStartMs = System.currentTimeMillis()
                var lastWm = WatermarkStore.read(oracleConn, t)
                WatermarkStore.beginRun(oracleConn, t, target.watermarkColumn, runId)
                oracleConn.commit()

                var totalLoaded = 0L
                var totalMerged = 0L
                var pageNum = 0
                var lastKeyValues: List<String?>? = null

                // Page loop: one iteration per staging+merge cycle. Terminates when
                // the source returns fewer rows than pageSize or when page-level rows
                // hit zero. Between pages only LAST_WM is persisted — outcome columns
                // are written once at end of run so mid-run UI state stays honest.
                do {
                    pageNum++
                    MDC.put("page", pageNum.toString())

                    val result = loadPageWithRetry(target, oracleConn, lastWm, lastKeyValues, pageNum)

                    if (result.rowsLoaded == 0L) break

                    val startMerge = System.currentTimeMillis()
                    val merged = TargetMerger.merge(oracleConn, target)
                    log.info("[{}] Merged {} rows in {}ms", t, merged, System.currentTimeMillis() - startMerge)
                    totalLoaded += result.rowsLoaded
                    totalMerged += merged
                    lastWm = result.newWm
                    lastKeyValues = result.lastKeyValues

                    WatermarkStore.updateProgress(oracleConn, t, lastWm, totalLoaded)
                    oracleConn.commit()

                    if (target.pageSize != null) {
                        log.info("[{}] Page {}: loaded={}, merged={} | Total: loaded={}, merged={} | wm={}",
                            t, pageNum, result.rowsLoaded, merged, totalLoaded, totalMerged, lastWm)
                    }
                } while (target.pageSize != null && result.rowsLoaded.toInt() == target.pageSize)

                val durationMs = System.currentTimeMillis() - runStartMs
                WatermarkStore.finishRunOk(oracleConn, t, totalLoaded, durationMs)
                oracleConn.commit()
                log.info("[{}] OK: total_loaded={}, total_merged={}, duration={}ms, new_wm={}",
                    t, totalLoaded, totalMerged, durationMs, lastWm)
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
        MDC.remove("target")
        MDC.remove("page")
        MDC.remove("runId")
    }

    /**
     * Wrap [StagingLoader.load] with a retry loop for transient source-auth failures.
     *
     * The source connection is borrowed from the shared [SourceDs] singleton via
     * [SourceDs.withConnection], which serialises source access across all workers
     * and handles auth-failure reconnect transparently (on the next invocation).
     * We retry up to [MAX_SOURCE_RETRIES] times on auth-like errors — staging is
     * truncated on entry so retries are idempotent. Any other exception propagates.
     */
    private fun loadPageWithRetry(
        target: TargetConfig,
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
                return SourceDs.withConnection { sourceConn ->
                    StagingLoader.load(target, sourceConn, oracleConn, lastWm, lastKeyValues)
                }
            } catch (e: Exception) {
                lastError = e
                if (attempt <= MAX_SOURCE_RETRIES && isSourceAuthError(e)) {
                    // SourceDs already discarded the connection internally; next
                    // withConnection call will re-authenticate. Just retry.
                    log.warn("[{}] Source error on attempt {}/{}: {}. Retrying (source will re-auth)...",
                        t, attempt, MAX_SOURCE_RETRIES + 1, e.message)
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
