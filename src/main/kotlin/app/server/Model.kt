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
 * Single row of the ETL_WATERMARK table surfaced as JSON for the status endpoint.
 *
 * Split into three logical sections:
 *  - **Progress**: [lastWm] advances per successful page (persists across runs).
 *  - **Current run**: [currentRunStarted]/[currentRunId] are populated only while
 *    a run is in flight. Both null means idle.
 *  - **Outcomes**: [lastSuccessFinished]/[lastSuccessRows] records the last OK run;
 *    [lastFailureFinished]/[lastFailureError] records the last FAILED run.
 *    They are independent — the UI compares the two timestamps to decide which
 *    outcome is "most recent" when the target is idle.
 *
 * All timestamps are ISO-8601 strings with trailing 'Z' (see WatermarkStore.toIsoUtc).
 */
@Serializable
data class WatermarkRow(
    val targetName: String,
    val lastWm: String?,
    val currentRunStarted: String?,
    val currentRunId: String?,
    val currentRunRows: Long,
    val lastSuccessFinished: String?,
    val lastSuccessRows: Long,
    val lastSuccessDurationMs: Long?,
    val lastFailureFinished: String?,
    val lastFailureError: String?
)

/**
 * Per-target snapshot combining in-memory running state with the persisted watermark
 * info. Powers the "what is this target doing right now?" column on the dashboard.
 */
@Serializable
data class TargetRunInfo(
    val name: String,
    val running: Boolean,
    val watermark: WatermarkRow?,
    val targetRowCount: Long? = null
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
