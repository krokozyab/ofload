package app.server

import app.db.OracleDs
import app.metrics.Metrics
import io.javalin.Javalin
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.util.concurrent.RejectedExecutionException

private val log = LoggerFactory.getLogger("app.server.Routes")
private val json = Json { prettyPrint = true }

/**
 * Wires the HTTP surface of the service: two HTML pages, a config/status pair for the
 * dashboard, /run* endpoints that trigger ETL, and /health for liveness probes.
 *
 * Nothing mutates state synchronously — /run* endpoints return 202 Accepted while the
 * actual ETL work runs on [RunManager]'s thread pool. This keeps the HTTP layer cheap
 * enough to sit behind a default-timeout load balancer while long-running loads proceed
 * in the background.
 */
fun configureRoutes(app: Javalin, runManager: RunManager, configHolder: ConfigHolder) {

    /** GET / — serves the dashboard HTML from the classpath. */
    app.get("/") { ctx ->
        val html = object {}.javaClass.getResourceAsStream("/static/dashboard.html")
        if (html != null) {
            ctx.contentType("text/html").result(html)
        } else {
            ctx.status(404).result("Dashboard not found")
        }
    }

    /** GET /targets — serves the read-only targets visualisation page. */
    app.get("/targets") { ctx ->
        val html = object {}.javaClass.getResourceAsStream("/static/targets-editor.html")
        if (html != null) {
            ctx.contentType("text/html").result(html)
        } else {
            ctx.status(404).result("Targets editor not found")
        }
    }

    /**
     * GET /config — thin target list (name + group) for the dashboard navigation.
     * Separate from /api/targets (full config) to keep the dashboard lightweight
     * and avoid shipping big SQL blobs to every browser tab.
     */
    app.get("/config") { ctx ->
        val targets = configHolder.config.targets.map { ConfigTarget(it.name, it.group) }
        ctx.status(200).result(json.encodeToString(ConfigResponse.serializer(), ConfigResponse(targets)))
    }

    /**
     * POST /run — kick off a pipeline run for every configured target.
     * Returns 202 on success, 409 if every target was already running,
     * 503 if the runner queue is full (bounded pool backpressure).
     */
    app.post("/run") { ctx ->
        try {
            val resp = runManager.submitAll()
            if (resp.targets.isEmpty()) ctx.status(409).result(encode(ErrorResponse(resp.message)))
            else ctx.status(202).result(encode(resp))
        } catch (e: RejectedExecutionException) {
            log.warn("/run rejected: runner queue full")
            ctx.status(503).result(encode(ErrorResponse("Runner queue full — try again later")))
        }
    }

    /**
     * POST /run/group/{groupId} — run only the targets of a specific group.
     * Returns 404 when no targets carry that group id, 409 when all of them were
     * already running, 503 on queue overflow, 202 on success.
     */
    app.post("/run/group/{groupId}") { ctx ->
        val groupId = ctx.pathParam("groupId").toIntOrNull()
        if (groupId == null) {
            ctx.status(400).result(encode(ErrorResponse("Invalid group ID")))
            return@post
        }
        try {
            val resp = runManager.submitGroup(groupId)
            if (resp == null) ctx.status(404).result(encode(ErrorResponse("No targets in group $groupId")))
            else if (resp.targets.isEmpty()) ctx.status(409).result(encode(ErrorResponse(resp.message)))
            else ctx.status(202).result(encode(resp))
        } catch (e: RejectedExecutionException) {
            log.warn("/run/group/{} rejected: runner queue full", groupId)
            ctx.status(503).result(encode(ErrorResponse("Runner queue full — try again later")))
        }
    }

    /**
     * POST /run/target/{name} — run a single target.
     * 404 if the name is unknown, 409 if already running, 503 on queue overflow,
     * 202 on success.
     */
    app.post("/run/target/{name}") { ctx ->
        val name = ctx.pathParam("name")
        try {
            val resp = runManager.submitTarget(name)
            if (resp == null) ctx.status(404).result(encode(ErrorResponse("Target '$name' not found")))
            else if (resp.targets.isEmpty()) ctx.status(409).result(encode(ErrorResponse(resp.message)))
            else ctx.status(202).result(encode(resp))
        } catch (e: RejectedExecutionException) {
            log.warn("/run/target/{} rejected: runner queue full", name)
            ctx.status(503).result(encode(ErrorResponse("Runner queue full — try again later")))
        }
    }

    /**
     * GET /live — pure liveness probe. Returns 200 as long as the JVM is alive and
     * serving HTTP. No DB access, no external dependencies. Intended for k8s
     * livenessProbe — flapping here means restart the pod.
     */
    app.get("/live") { ctx ->
        ctx.status(200).result(encode(HealthResponse(status = "ok", db = "n/a")))
    }

    /**
     * GET /ready — readiness probe. Returns 200 only when we can actually serve ETL
     * traffic: target DB reachable AND source driver registered. 503 otherwise.
     * Intended for k8s readinessProbe — failures here take the pod out of rotation
     * without restarting it.
     *
     * Source check is intentionally lightweight (env vars + driver class loaded) to
     * avoid triggering a full SSO handshake on every probe.
     */
    app.get("/ready") { ctx ->
        val dbOk = try {
            OracleDs.getConnection().use { conn ->
                conn.createStatement().use { it.execute("SELECT 1 FROM DUAL") }
            }
            true
        } catch (e: Exception) {
            log.warn("/ready target-db check failed: {}", e.message)
            false
        }
        val sourceOk = System.getenv("SOURCE_URL") != null &&
                       System.getenv("SOURCE_DRIVER_CLASS") != null &&
                       runCatching { Class.forName(System.getenv("SOURCE_DRIVER_CLASS")) }.isSuccess

        if (dbOk && sourceOk) {
            ctx.status(200).result(encode(HealthResponse(status = "ready", db = "connected")))
        } else {
            val reason = buildString {
                if (!dbOk) append("target-db unreachable; ")
                if (!sourceOk) append("source driver unavailable; ")
            }
            ctx.status(503).result(encode(HealthResponse(status = "degraded",
                db = if (dbOk) "connected" else "error",
                error = reason.trimEnd(' ', ';'))))
        }
    }

    /**
     * GET /metrics — Prometheus scrape endpoint. Returns the text-format snapshot of
     * all counters, timers and gauges registered via [Metrics]. Designed to be
     * polled by a Prometheus server or compatible scraper every 15–60s.
     */
    app.get("/metrics") { ctx ->
        ctx.contentType("text/plain; version=0.0.4; charset=utf-8").result(Metrics.scrape())
    }

    /** GET /health — backward-compat alias of /ready (existing dashboard polls /health). */
    app.get("/health") { ctx ->
        try {
            OracleDs.getConnection().use { conn ->
                conn.createStatement().use { it.execute("SELECT 1 FROM DUAL") }
            }
            ctx.status(200).result(encode(HealthResponse(status = "ok", db = "connected")))
        } catch (e: Exception) {
            log.warn("Health check DB failed: {}", e.message)
            ctx.status(503).result(encode(HealthResponse(status = "degraded", db = "error", error = e.message)))
        }
    }

    /**
     * GET /status — consolidated run state for the dashboard.
     * Merges in-memory "running now" flags with persisted watermarks so the UI can
     * display both the live state and the last persisted outcome per target.
     */
    app.get("/status") { ctx ->
        val status = runManager.getStatus()
        ctx.status(200).result(encode(status))
    }
}

/* Overloaded encoders — kotlinx.serialization requires an explicit serializer per type
 * at the call site, so we keep one encode-per-DTO helper to keep endpoint handlers tidy. */
private fun encode(resp: RunResponse) = json.encodeToString(RunResponse.serializer(), resp)
private fun encode(resp: ErrorResponse) = json.encodeToString(ErrorResponse.serializer(), resp)
private fun encode(resp: HealthResponse) = json.encodeToString(HealthResponse.serializer(), resp)
private fun encode(resp: StatusResponse) = json.encodeToString(StatusResponse.serializer(), resp)
private fun encode(resp: ConfigResponse) = json.encodeToString(ConfigResponse.serializer(), resp)
