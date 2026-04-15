package app.server

import kotlinx.serialization.Serializable

@Serializable
data class RunResponse(
    val runId: String,
    val targets: List<String>,
    val skipped: List<String> = emptyList(),
    val message: String
)

@Serializable
data class ErrorResponse(val error: String)

@Serializable
data class HealthResponse(
    val status: String,
    val db: String,
    val error: String? = null
)

@Serializable
data class WatermarkRow(
    val targetName: String,
    val lastWm: String?,
    val lastStatus: String?,
    val lastRunStarted: String?,
    val lastRunFinished: String?,
    val rowsLoaded: Long,
    val errorMessage: String?
)

@Serializable
data class TargetRunInfo(
    val name: String,
    val running: Boolean,
    val watermark: WatermarkRow?
)

@Serializable
data class StatusResponse(
    val targets: List<TargetRunInfo>
)

@Serializable
data class ConfigTarget(
    val name: String,
    val group: Int?
)

@Serializable
data class ConfigResponse(
    val targets: List<ConfigTarget>
)
