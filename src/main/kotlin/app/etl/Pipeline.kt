package app.etl

import app.config.PipelineConfig
import app.config.TargetConfig
import app.db.OracleDs
import app.db.SourceDs
import app.metrics.Metrics
import my.jdbc.wsdl_driver.auth.TokenExpiredException
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
 * **Groups** have two orthogonal roles:
 *   1. **Parallel execution within a group** — targets sharing a group id are
 *      submitted to the shared [etlPool] and run concurrently.
 *   2. **Sequential barrier between groups** — group N+1 does not start until
 *      group N has fully completed (useful for dimension-then-fact ordering).
 *
 * Each worker opens its own source connection via [SourceDs.withConnection]. The
 * driver's in-process `TokenCache` means only the first worker triggers interactive
 * SSO; subsequent workers reuse the cached JWT silently. Source reads and ATP-side
 * merges therefore run truly in parallel across targets in a group.
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

    init {
        // Expose pool active/queue gauges to Prometheus on `pool=etl` series.
        Metrics.registerPoolGauges("etl", etlPool)
    }

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
                log.info("Group {}: running {} in parallel (each with its own source connection, SSO shared via driver TokenCache)",
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
     * mark run as OK. Each page borrows a fresh source connection via
     * [loadPageWithRetry] → [SourceDs.withConnection]; the driver's TokenCache
     * ensures no extra SSO browser opens when multiple workers create connections
     * in parallel.
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
            var runStatus = "failed"  // Updated to "ok" on successful completion.
            val runStartMs = System.currentTimeMillis()
            try {
                var lastWm = WatermarkStore.read(oracleConn, target)
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

                    WatermarkStore.updateProgress(oracleConn, target, lastWm, totalLoaded)
                    oracleConn.commit()

                    if (target.pageSize != null) {
                        log.info("[{}] Page {}: loaded={}, merged={} | Total: loaded={}, merged={} | wm={}",
                            t, pageNum, result.rowsLoaded, merged, totalLoaded, totalMerged, lastWm)
                    }
                } while (target.pageSize != null && result.rowsLoaded.toInt() == target.pageSize)

                val durationMs = System.currentTimeMillis() - runStartMs
                WatermarkStore.finishRunOk(oracleConn, t, totalLoaded, durationMs)
                oracleConn.commit()
                runStatus = "ok"
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
            } finally {
                // Metrics: record run outcome + duration regardless of success/failure.
                val totalMs = System.currentTimeMillis() - runStartMs
                Metrics.runTimer(t, runStatus).record(java.time.Duration.ofMillis(totalMs))
                Metrics.runCompleted(t, runStatus)
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
                    Metrics.authRetry(t)
                } else {
                    throw e
                }
            }
        }
        throw lastError!!
    }

    /**
     * Classifier for "is this a transient SSO/auth failure worth retrying?".
     *
     * **Primary signal** — instance check for [TokenExpiredException] anywhere in the
     * exception cause chain. The driver raises this type with a `statusCode` field for
     * all 401-class responses (including "Token lacks SOAP privileges"), and
     * `PaginatedResultSet.fetchPageXml` wraps it in `SQLTimeoutException(cause=ex)`
     * after retry exhaustion. Walking `cause` gets us the original classification
     * independently of whatever wording the driver chooses in the wrapper.
     *
     * **Fallback** — substring match across the aggregated message chain. Covers:
     *   - `"Authentication failed - invalid username or password"` raised by
     *     `HttpClient.kt` for non-token auth failures (no dedicated exception type yet).
     *   - Any future driver wrapping that hides the [TokenExpiredException] type.
     *   - Generic "session closed" / "login required" variants.
     *
     * Bias is toward false-positive retry — cost is one extra SSO reconnect; cost of a
     * false-negative is losing a run that could have recovered.
     *
     * Internal (not private) so unit tests in the same module can exercise the matcher.
     */
    internal fun isSourceAuthError(e: Throwable): Boolean {
        // Primary: instance check on the cause chain.
        var current: Throwable? = e
        while (current != null) {
            if (current is TokenExpiredException) return true
            current = current.cause
        }
        // Fallback: aggregate messages across the cause chain and substring-match.
        val messages = buildString {
            var c: Throwable? = e
            while (c != null) {
                c.message?.let { append(it).append(' ') }
                c = c.cause
            }
        }.lowercase()
        return messages.contains("401") ||
               messages.contains("unauthorized") ||
               messages.contains("authentication") ||
               messages.contains("token") ||
               messages.contains("session") ||
               messages.contains("login")
    }
}
