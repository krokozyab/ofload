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

class RunManager(private val configHolder: ConfigHolder, private val pipeline: Pipeline) {
    private val log = LoggerFactory.getLogger(RunManager::class.java)
    private val executor = Executors.newFixedThreadPool(
        (System.getenv("RUNNER_THREADS")?.toIntOrNull() ?: 4)
    )
    private val runningTargets = ConcurrentHashMap.newKeySet<String>()

    fun isRunning(name: String): Boolean = name in runningTargets

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

    fun shutdown() {
        log.info("RunManager shutting down, waiting for in-flight runs...")
        executor.shutdown()
        if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
            log.warn("Forcing shutdown, {} targets still running", runningTargets.size)
            executor.shutdownNow()
        }
    }

    private fun lockTargets(targets: List<TargetConfig>): Pair<List<TargetConfig>, List<TargetConfig>> {
        val toRun = mutableListOf<TargetConfig>()
        val skipped = mutableListOf<TargetConfig>()
        for (t in targets) {
            if (runningTargets.add(t.name)) toRun.add(t) else skipped.add(t)
        }
        return toRun to skipped
    }
}
