package app.server

import app.config.PipelineConfig
import app.config.TargetConfig
import app.db.OracleDs
import app.etl.Pipeline
import app.etl.WatermarkStore
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
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
     * Fixed thread pool for ETL runs. Sized via RUNNER_THREADS env var (default 4).
     * Bounded on purpose — unbounded submission would overload Oracle ATP.
     */
    private val executor = Executors.newFixedThreadPool(
        (System.getenv("RUNNER_THREADS")?.toIntOrNull() ?: 4)
    )

    /**
     * Names of targets currently executing. Used as the concurrency lock that
     * prevents overlapping runs of the same target. Thread-safe (ConcurrentHashMap-backed set).
     */
    private val runningTargets = ConcurrentHashMap.newKeySet<String>()

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
        executor.submit {
            try {
                pipeline.runTargets(toRun)
            } finally {
                toRun.forEach { runningTargets.remove(it.name) }
            }
        }
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
        executor.submit {
            try {
                pipeline.runTargets(toRun)
            } finally {
                toRun.forEach { runningTargets.remove(it.name) }
            }
        }
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
        executor.submit {
            try {
                pipeline.runTargets(listOf(target))
            } finally {
                runningTargets.remove(name)
            }
        }
        return RunResponse(runId, listOf(name), emptyList(), "Target $name started")
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
        val watermarks = try {
            OracleDs.getConnection().use { conn -> WatermarkStore.readAll(conn) }
        } catch (e: Exception) {
            log.warn("Failed to read watermarks for status: {}", e.message)
            emptyList()
        }
        val wmByName = watermarks.associateBy { it.targetName }

        val targets = configHolder.config.targets.map { t ->
            TargetRunInfo(
                name = t.name,
                running = t.name in runningTargets,
                watermark = wmByName[t.name]
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
