package app

import app.config.loadConfig
import app.etl.Pipeline

fun main() {
    val configStream = object {}.javaClass.getResourceAsStream("/targets.json")
        ?: error("targets.json not found in resources")
    val config = loadConfig(configStream)
    println("Loaded ${config.targets.size} target(s): ${config.targets.map { it.name }}")
    Pipeline(config).run()
    println("Pipeline finished.")
}
