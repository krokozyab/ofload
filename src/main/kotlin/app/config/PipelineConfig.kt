package app.config

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.InputStream

@Serializable
data class TargetConfig(
    val name: String,
    val stagingTable: String,
    val targetTable: String,
    val watermarkColumn: String,
    val watermarkType: String,       // "TIMESTAMP" | "NUMBER"
    val keyColumns: List<String>,
    val columns: List<String>,
    val sourceQuery: String,
    val initialWm: String,
    val pageSize: Int? = null,
    val lookbackMinutes: Long? = null,
    val group: Int? = null
)

@Serializable
data class PipelineConfig(val targets: List<TargetConfig>)

fun loadConfig(stream: InputStream): PipelineConfig =
    Json.decodeFromString(PipelineConfig.serializer(), stream.bufferedReader().readText())
