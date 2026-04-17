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

/**
 * Service entry point.
 *
 * Loads targets.json from the classpath, wires the in-memory [ConfigHolder],
 * [Pipeline] and [RunManager], cleans up any stale RUNNING watermarks left behind
 * by previous crashed runs, then starts the Javalin HTTP server that exposes the
 * dashboard, the read-only targets page, and the /run endpoints. A shutdown hook
 * gives in-flight ETL runs up to 5 minutes to finish before forcing exit.
 */
fun main() {
    val configStream = object {}.javaClass.getResourceAsStream("/targets.json")
        ?: error("targets.json not found in resources")
    val config = loadConfig(configStream)
    log.info("Loaded {} target(s): {}", config.targets.size, config.targets.map { it.name })

    val configHolder = ConfigHolder(config)
    val pipeline = Pipeline(config)
    val runManager = RunManager(configHolder, pipeline)

    // Reset stale RUNNING targets left by a previous crashed JVM.
    // If this fails the service still starts — the watermark layer is advisory
    // and the pipeline will overwrite stale state on the next successful run.
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

    // Give in-flight runs a chance to finish cleanly on SIGTERM/SIGINT.
    Runtime.getRuntime().addShutdownHook(Thread {
        log.info("Shutting down...")
        app.stop()
        runManager.shutdown()
    })

    log.info("Service started on port {}", port)
}
