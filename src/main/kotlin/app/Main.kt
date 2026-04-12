package app

import app.config.loadConfig
import app.etl.Pipeline
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("app.Main")

fun main() {
    val configStream = object {}.javaClass.getResourceAsStream("/targets.json")
        ?: error("targets.json not found in resources")
    val config = loadConfig(configStream)
    log.info("Loaded {} target(s): {}", config.targets.size, config.targets.map { it.name })
    Pipeline(config).run()
    log.info("Pipeline finished.")
}
