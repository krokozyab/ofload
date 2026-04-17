package app.server

import kotlinx.serialization.Serializable

/**
 * Success body returned by the /run*, /run/group/{id} and /run/target/{name} endpoints.
 *
 * @property runId UUID issued per submission so operators can correlate logs with a
 *   specific API call. Empty string when the request was a no-op (all targets busy).
 * @property targets target names actually started by this call.
 * @property skipped target names that were rejected because they were already running.
 * @property message short human-readable summary for the UI toast.
 */
@Serializable
data class RunResponse(
    val runId: String,
    val targets: List<String>,
    val skipped: List<String> = emptyList(),
    val message: String
)

/** Generic error body — `{ "error": "..." }` — used by every failing endpoint. */
@Serializable
data class ErrorResponse(val error: String)

/**
 * Response body for /health. Reports "ok"/"degraded" plus a DB connectivity probe so
 * ops tooling (load balancers, uptime checks) can distinguish "process alive but DB
 * unreachable" from "process down".
 */
@Serializable
data class HealthResponse(
    val status: String,
    val db: String,
    val error: String? = null
)

/**
 * Single row of the ETL_WATERMARK table surfaced as JSON for the status endpoint
 * and the dashboard UI. All fields come straight from the table — timestamps are
 * kept as strings to avoid timezone-conversion surprises between Oracle, JVM and JS.
 */
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

/**
 * Per-target snapshot combining in-memory running state with the persisted watermark
 * info. Powers the "what is this target doing right now?" column on the dashboard.
 */
@Serializable
data class TargetRunInfo(
    val name: String,
    val running: Boolean,
    val watermark: WatermarkRow?
)

/** Payload for /status — one [TargetRunInfo] per configured target. */
@Serializable
data class StatusResponse(
    val targets: List<TargetRunInfo>
)

/**
 * Minimal target descriptor for /config — just the name and group, so the dashboard
 * can render the target list and group filter without pulling the full config
 * (which includes SQL blobs that the dashboard does not need).
 */
@Serializable
data class ConfigTarget(
    val name: String,
    val group: Int?
)

/** Payload for /config — thin target list used by the dashboard navigation. */
@Serializable
data class ConfigResponse(
    val targets: List<ConfigTarget>
)
