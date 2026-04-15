package app

import app.config.loadConfig
import app.db.OracleDs
import app.etl.Pipeline
import app.etl.WatermarkStore
import app.server.ConfigHolder
import app.server.RunManager
import app.server.configureRoutes
import app.server.configureTargetsApi
import io.javalin.Javalin
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("app.Main")

fun main() {
    val configPath = System.getenv("TARGETS_CONFIG")
        ?: "src/main/resources/targets.json"

    val configStream = object {}.javaClass.getResourceAsStream("/targets.json")
        ?: error("targets.json not found in resources")
    val config = loadConfig(configStream)
    log.info("Loaded {} target(s): {}", config.targets.size, config.targets.map { it.name })

    val configHolder = ConfigHolder(config, configPath)
    val pipeline = Pipeline(config)
    val runManager = RunManager(configHolder, pipeline)

    // Reset stale RUNNING targets from previous crashed runs
    try {
        OracleDs.getConnection().use { conn ->
            conn.autoCommit = false
            WatermarkStore.resetStaleRunning(conn)
        }
    } catch (e: Exception) {
        log.warn("Could not reset stale watermarks: {}", e.message)
    }

    val port = System.getenv("PORT")?.toIntOrNull() ?: 8080
    val app = Javalin.create { cfg ->
        cfg.showJavalinBanner = false
    }.start(port)

    configureRoutes(app, runManager, configHolder)
    configureTargetsApi(app, configHolder)

    Runtime.getRuntime().addShutdownHook(Thread {
        log.info("Shutting down...")
        app.stop()
        runManager.shutdown()
    })

    log.info("Service started on port {}", port)
}
