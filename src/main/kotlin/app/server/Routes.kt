package app.server

import app.db.OracleDs
import io.javalin.Javalin
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("app.server.Routes")
private val json = Json { prettyPrint = true }

fun configureRoutes(app: Javalin, runManager: RunManager, configHolder: ConfigHolder) {

    // Dashboard
    app.get("/") { ctx ->
        val html = object {}.javaClass.getResourceAsStream("/static/dashboard.html")
        if (html != null) {
            ctx.contentType("text/html").result(html)
        } else {
            ctx.status(404).result("Dashboard not found")
        }
    }

    // Targets editor page
    app.get("/targets") { ctx ->
        val html = object {}.javaClass.getResourceAsStream("/static/targets-editor.html")
        if (html != null) {
            ctx.contentType("text/html").result(html)
        } else {
            ctx.status(404).result("Targets editor not found")
        }
    }

    // Config endpoint for dashboard (target names + groups)
    app.get("/config") { ctx ->
        val targets = configHolder.config.targets.map { ConfigTarget(it.name, it.group) }
        ctx.status(200).result(json.encodeToString(ConfigResponse.serializer(), ConfigResponse(targets)))
    }

    app.post("/run") { ctx ->
        val resp = runManager.submitAll()
        if (resp.targets.isEmpty()) {
            ctx.status(409).result(encode(ErrorResponse(resp.message)))
        } else {
            ctx.status(202).result(encode(resp))
        }
    }

    app.post("/run/group/{groupId}") { ctx ->
        val groupId = ctx.pathParam("groupId").toIntOrNull()
        if (groupId == null) {
            ctx.status(400).result(encode(ErrorResponse("Invalid group ID")))
            return@post
        }
        val resp = runManager.submitGroup(groupId)
        if (resp == null) {
            ctx.status(404).result(encode(ErrorResponse("No targets in group $groupId")))
            return@post
        }
        if (resp.targets.isEmpty()) {
            ctx.status(409).result(encode(ErrorResponse(resp.message)))
        } else {
            ctx.status(202).result(encode(resp))
        }
    }

    app.post("/run/target/{name}") { ctx ->
        val name = ctx.pathParam("name")
        val resp = runManager.submitTarget(name)
        if (resp == null) {
            ctx.status(404).result(encode(ErrorResponse("Target '$name' not found")))
            return@post
        }
        if (resp.targets.isEmpty()) {
            ctx.status(409).result(encode(ErrorResponse(resp.message)))
        } else {
            ctx.status(202).result(encode(resp))
        }
    }

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

    app.get("/status") { ctx ->
        val status = runManager.getStatus()
        ctx.status(200).result(encode(status))
    }
}

private fun encode(resp: RunResponse) = json.encodeToString(RunResponse.serializer(), resp)
private fun encode(resp: ErrorResponse) = json.encodeToString(ErrorResponse.serializer(), resp)
private fun encode(resp: HealthResponse) = json.encodeToString(HealthResponse.serializer(), resp)
private fun encode(resp: StatusResponse) = json.encodeToString(StatusResponse.serializer(), resp)
private fun encode(resp: ConfigResponse) = json.encodeToString(ConfigResponse.serializer(), resp)
