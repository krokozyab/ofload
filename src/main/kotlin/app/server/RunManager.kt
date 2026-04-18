package app.server

import app.config.PipelineConfig
import app.config.TargetConfig
import app.db.OracleDs
import app.etl.Pipeline
import app.etl.WatermarkStore
import app.metrics.Metrics
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * Orchestrates on-demand ETL runs submitted via the HTTP API.
 *
 * Owns a bounded thread pool that executes [Pipeline.runTargets] off the request
 * thread, and tracks which targets are currently running so overlapping submissions
 * for the same target are rejected (each target is serialised — never two concurrent
 * runs of the same target).
 */
class RunManager(private val configHolder: ConfigHolder, private val pipeline: Pipeline) {
    private val log = LoggerFactory.getLogger(RunManager::class.java)

    /**
     * Bounded thread pool for ETL submissions from the HTTP layer.
     *
     *  - `RUNNER_THREADS` (env, default 4) — how many concurrent /run* submissions we accept
     *  - `RUNNER_QUEUE_SIZE` (env, default 16) — pending submissions held before we reject
     *
     * On overflow AbortPolicy throws [RejectedExecutionException] so the HTTP layer
     * can surface a 503 with a clear "queue full" message instead of silently
     * queueing work no operator can see or cancel.
     */
    private val runnerThreads = (System.getenv("RUNNER_THREADS")?.toIntOrNull() ?: 4)
    private val runnerQueueSize = (System.getenv("RUNNER_QUEUE_SIZE")?.toIntOrNull() ?: 16)
    private val executor = ThreadPoolExecutor(
        runnerThreads, runnerThreads,
        0L, TimeUnit.MILLISECONDS,
        ArrayBlockingQueue<Runnable>(runnerQueueSize),
        ThreadPoolExecutor.AbortPolicy()
    ).also { Metrics.registerPoolGauges("runner", it) }

    /**
     * Names of targets currently executing. Used as the concurrency lock that
     * prevents overlapping runs of the same target. Thread-safe (ConcurrentHashMap-backed set).
     */
    private val runningTargets = ConcurrentHashMap.newKeySet<String>()

    /**
     * TTL cache for target-table row counts surfaced on `/status`. Refreshes happen
     * on a background thread so slow COUNT(*) queries don't block the HTTP handler.
     */
    private val rowCountCache = TargetRowCountCache()

    /**
     * Non-blocking check for UI/status endpoints: is this target mid-run right now?
     */
    fun isRunning(name: String): Boolean = name in runningTargets

    /**
     * Submit every target defined in the current config for execution.
     *
     * Targets that are already running are reported in `skipped` and not re-submitted.
     * Returns immediately after handing the work to the executor — callers do not block
     * on pipeline completion.
     */
    fun submitAll(): RunResponse {
        val (toRun, skipped) = lockTargets(configHolder.config.targets)
        if (toRun.isEmpty()) return RunResponse("", emptyList(), skipped.map { it.name }, "All targets already running")

        val runId = UUID.randomUUID().toString()
        submitToExecutor(toRun) { pipeline.runTargets(toRun, runId) }
        return RunResponse(runId, toRun.map { it.name }, skipped.map { it.name }, "Pipeline started")
    }

    /**
     * Submit only the targets belonging to the given `group` id (from targets.json).
     *
     * Groups are the unit of source-side batching: targets in the same group share one
     * Fusion SSO login in [Pipeline.runTargets]. Returns null if the group has no members,
     * so the HTTP layer can return 404.
     */
    fun submitGroup(groupId: Int): RunResponse? {
        val targets = configHolder.config.targets.filter { it.group == groupId }
        if (targets.isEmpty()) return null

        val (toRun, skipped) = lockTargets(targets)
        if (toRun.isEmpty()) return RunResponse("", emptyList(), skipped.map { it.name }, "All targets in group $groupId already running")

        val runId = UUID.randomUUID().toString()
        submitToExecutor(toRun) { pipeline.runTargets(toRun, runId) }
        return RunResponse(runId, toRun.map { it.name }, skipped.map { it.name }, "Group $groupId started")
    }

    /**
     * Submit a single target by name.
     *
     * Returns null if no target with that name exists (HTTP layer maps to 404).
     * If the target is already running, responds with the target listed in `skipped`
     * so the caller can distinguish "not found" from "already in progress".
     */
    fun submitTarget(name: String): RunResponse? {
        val target = configHolder.config.targets.find { it.name == name } ?: return null
        if (!runningTargets.add(name)) {
            return RunResponse("", emptyList(), listOf(name), "Target $name already running")
        }

        val runId = UUID.randomUUID().toString()
        submitToExecutor(listOf(target)) { pipeline.runTargets(listOf(target), runId) }
        return RunResponse(runId, listOf(name), emptyList(), "Target $name started")
    }

    /**
     * Wraps executor.submit with lock-rollback on queue-full rejection.
     *
     * The RunManager's pool is bounded (see executor). If the caller asks to run
     * while the pool's queue is full, AbortPolicy throws [RejectedExecutionException]
     * and we must un-lock the targets we just reserved before propagating, otherwise
     * those targets would be stuck in runningTargets forever. The exception then
     * reaches the HTTP layer which maps it to 503 with a clear message.
     */
    private fun submitToExecutor(toRun: List<TargetConfig>, work: () -> Unit) {
        try {
            executor.submit {
                try { work() } finally { toRun.forEach { runningTargets.remove(it.name) } }
            }
        } catch (e: RejectedExecutionException) {
            toRun.forEach { runningTargets.remove(it.name) }
            Metrics.runnerRejection()
            throw e
        }
    }

    /**
     * Snapshot of every target's live state for the status endpoint.
     *
     * Combines in-memory running state (per-process) with persisted watermark
     * info from ETL_WATERMARK (per-target last wm/status/error). If the DB is
     * unreachable the watermark side degrades to empty — the UI still gets
     * running/not-running so the operator can see what the process thinks is active.
     */
    fun getStatus(): StatusResponse {
        // Watermarks — fast single query, read synchronously. Row counts come from
        // the TTL cache (never blocks; async background refresh absorbs latency).
        val watermarks = try {
            OracleDs.getConnection().use { conn ->
                WatermarkStore.readAll(conn, configHolder.config)
            }
        } catch (e: Exception) {
            log.warn("Failed to read watermarks for status: {}", e.message)
            emptyList()
        }
        val wmByName = watermarks.associateBy { it.targetName }

        val targets = configHolder.config.targets.map { t ->
            TargetRunInfo(
                name = t.name,
                running = t.name in runningTargets,
                watermark = wmByName[t.name],
                targetRowCount = rowCountCache.get(t.name, t.targetTable)
            )
        }
        return StatusResponse(targets)
    }

    /**
     * Graceful shutdown on JVM exit: stop accepting new submissions, drain in-flight
     * runs (up to 5 minutes), then tear down the Pipeline's ETL worker pool too.
     * Shuts pools down in dependency order — outer submission pool first, so no new
     * target work can be queued; then Pipeline's inner pool once remaining work drains.
     * Prevents half-committed ETL state caused by killing the process mid-merge.
     */
    fun shutdown() {
        log.info("RunManager shutting down, waiting for in-flight runs...")
        executor.shutdown()
        if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
            log.warn("Forcing shutdown, {} targets still running", runningTargets.size)
            executor.shutdownNow()
        }
        rowCountCache.close()
        pipeline.shutdown()
    }

    /**
     * Atomically reserve the given targets for execution.
     *
     * Uses [runningTargets].add() — a thread-safe single-op compare-and-insert — to
     * decide, per target, whether this caller wins the race or another submission
     * already holds the slot. Targets that win go into `toRun`; those that lose go
     * into `skipped`. Callers are responsible for removing entries from [runningTargets]
     * once their run finishes (see try/finally blocks in submit* methods).
     */
    private fun lockTargets(targets: List<TargetConfig>): Pair<List<TargetConfig>, List<TargetConfig>> {
        val toRun = mutableListOf<TargetConfig>()
        val skipped = mutableListOf<TargetConfig>()
        for (t in targets) {
            if (runningTargets.add(t.name)) toRun.add(t) else skipped.add(t)
        }
        return toRun to skipped
    }
}
