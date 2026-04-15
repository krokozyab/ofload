package app.server

import app.config.PipelineConfig
import app.config.TargetConfig
import app.db.OracleDs
import app.etl.WatermarkStore
import io.javalin.Javalin
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.io.File

private val log = LoggerFactory.getLogger("app.server.TargetsApi")
private val json = Json { prettyPrint = true; ignoreUnknownKeys = true }

@Serializable
data class DdlResponse(
    val stagingDdl: String,
    val targetDdl: String,
    val stagingTableExists: Boolean,
    val targetTableExists: Boolean
)

@Serializable
data class MessageResponse(val message: String)

fun configureTargetsApi(app: Javalin, configHolder: ConfigHolder) {

    // GET /api/targets — list all targets
    app.get("/api/targets") { ctx ->
        ctx.status(200).result(json.encodeToString(PipelineConfig.serializer(), configHolder.config))
    }

    // GET /api/targets/{name} — get single target
    app.get("/api/targets/{name}") { ctx ->
        val name = ctx.pathParam("name")
        val target = configHolder.config.targets.find { it.name == name }
        if (target == null) {
            ctx.status(404).result(json.encodeToString(ErrorResponse.serializer(), ErrorResponse("Target '$name' not found")))
        } else {
            ctx.status(200).result(json.encodeToString(TargetConfig.serializer(), target))
        }
    }

    // POST /api/targets — create new target
    app.post("/api/targets") { ctx ->
        val target = json.decodeFromString(TargetConfig.serializer(), ctx.body())
        if (configHolder.config.targets.any { it.name == target.name }) {
            ctx.status(409).result(json.encodeToString(ErrorResponse.serializer(), ErrorResponse("Target '${target.name}' already exists")))
            return@post
        }
        val newConfig = PipelineConfig(configHolder.config.targets + target)
        configHolder.saveConfig(newConfig)
        log.info("Target created: {}", target.name)
        ctx.status(201).result(json.encodeToString(TargetConfig.serializer(), target))
    }

    // PUT /api/targets/{name} — update target
    app.put("/api/targets/{name}") { ctx ->
        val name = ctx.pathParam("name")
        val target = json.decodeFromString(TargetConfig.serializer(), ctx.body())
        val existing = configHolder.config.targets.find { it.name == name }
        if (existing == null) {
            ctx.status(404).result(json.encodeToString(ErrorResponse.serializer(), ErrorResponse("Target '$name' not found")))
            return@put
        }
        val newTargets = configHolder.config.targets.map { if (it.name == name) target else it }
        configHolder.saveConfig(PipelineConfig(newTargets))
        log.info("Target updated: {}", name)
        ctx.status(200).result(json.encodeToString(TargetConfig.serializer(), target))
    }

    // DELETE /api/targets/{name}
    app.delete("/api/targets/{name}") { ctx ->
        val name = ctx.pathParam("name")
        val target = configHolder.config.targets.find { it.name == name }
        if (target == null) {
            ctx.status(404).result(json.encodeToString(ErrorResponse.serializer(), ErrorResponse("Target '$name' not found")))
            return@delete
        }

        val dropTables = ctx.queryParam("dropTables")?.toBoolean() ?: false
        val dropWatermark = ctx.queryParam("dropWatermark")?.toBoolean() ?: false

        if (dropTables) {
            try {
                OracleDs.getConnection().use { conn ->
                    conn.createStatement().use { st ->
                        runCatching { st.execute("DROP TABLE ${target.stagingTable}") }
                            .onSuccess { log.info("Dropped {}", target.stagingTable) }
                            .onFailure { log.warn("Drop {} failed: {}", target.stagingTable, it.message) }
                        runCatching { st.execute("DROP TABLE ${target.targetTable}") }
                            .onSuccess { log.info("Dropped {}", target.targetTable) }
                            .onFailure { log.warn("Drop {} failed: {}", target.targetTable, it.message) }
                    }
                }
            } catch (e: Exception) {
                log.error("Failed to drop tables for {}: {}", name, e.message)
            }
        }

        if (dropWatermark) {
            try {
                OracleDs.getConnection().use { conn ->
                    conn.prepareStatement("DELETE FROM ETL_WATERMARK WHERE TARGET_NAME = ?").use { ps ->
                        ps.setString(1, name)
                        ps.executeUpdate()
                    }
                    conn.commit()
                    log.info("Deleted watermark for {}", name)
                }
            } catch (e: Exception) {
                log.error("Failed to delete watermark for {}: {}", name, e.message)
            }
        }

        val newConfig = PipelineConfig(configHolder.config.targets.filter { it.name != name })
        configHolder.saveConfig(newConfig)
        log.info("Target deleted: {}", name)
        ctx.status(200).result(json.encodeToString(MessageResponse.serializer(), MessageResponse("Target '$name' deleted")))
    }

    // GET /api/targets/{name}/ddl — generate DDL
    app.get("/api/targets/{name}/ddl") { ctx ->
        val name = ctx.pathParam("name")
        val target = configHolder.config.targets.find { it.name == name }
        if (target == null) {
            ctx.status(404).result(json.encodeToString(ErrorResponse.serializer(), ErrorResponse("Target '$name' not found")))
            return@get
        }

        // Check if tables exist
        var stagingExists = false
        var targetExists = false
        try {
            OracleDs.getConnection().use { conn ->
                stagingExists = tableExists(conn, target.stagingTable)
                targetExists = tableExists(conn, target.targetTable)
            }
        } catch (e: Exception) {
            log.warn("Failed to check table existence: {}", e.message)
        }

        // Generate DDL from DB metadata
        var stagingDdl = "-- Table ${target.stagingTable} does not exist"
        var targetDdl = "-- Table ${target.targetTable} does not exist"
        try {
            OracleDs.getConnection().use { conn ->
                if (stagingExists) stagingDdl = generateDdlFromDb(conn, target.stagingTable)
                if (targetExists) targetDdl = generateDdlFromDb(conn, target.targetTable)
            }
        } catch (e: Exception) {
            log.warn("Failed to generate DDL: {}", e.message)
        }

        ctx.status(200).result(json.encodeToString(DdlResponse.serializer(),
            DdlResponse(stagingDdl, targetDdl, stagingExists, targetExists)))
    }

    // POST /api/targets/{name}/ddl/execute — execute DDL
    app.post("/api/targets/{name}/ddl/execute") { ctx ->
        val name = ctx.pathParam("name")
        val target = configHolder.config.targets.find { it.name == name }
        if (target == null) {
            ctx.status(404).result(json.encodeToString(ErrorResponse.serializer(), ErrorResponse("Target '$name' not found")))
            return@post
        }

        // Accept DDL from request body or generate from files
        val body = if (ctx.body().isNotBlank()) {
            try { json.decodeFromString(DdlExecuteRequest.serializer(), ctx.body()) } catch (e: Exception) { null }
        } else null

        val stagingDdl = body?.stagingDdl
        val targetDdl = body?.targetDdl

        val results = mutableListOf<String>()
        try {
            OracleDs.getConnection().use { conn ->
                conn.createStatement().use { st ->
                    if (stagingDdl != null) {
                        st.execute(stagingDdl)
                        results.add("${target.stagingTable} created")
                        log.info("DDL executed: {} created", target.stagingTable)
                    }
                    if (targetDdl != null) {
                        st.execute(targetDdl)
                        results.add("${target.targetTable} created")
                        log.info("DDL executed: {} created", target.targetTable)
                    }
                }
            }
            ctx.status(200).result(json.encodeToString(MessageResponse.serializer(),
                MessageResponse(results.joinToString(", "))))
        } catch (e: Exception) {
            log.error("DDL execution failed for {}: {}", name, e.message)
            ctx.status(500).result(json.encodeToString(ErrorResponse.serializer(),
                ErrorResponse("DDL failed: ${e.message}")))
        }
    }
}

@Serializable
data class DdlExecuteRequest(
    val stagingDdl: String? = null,
    val targetDdl: String? = null
)

private fun tableExists(conn: java.sql.Connection, tableName: String): Boolean {
    conn.prepareStatement("SELECT 1 FROM user_tables WHERE table_name = ?").use { ps ->
        ps.setString(1, tableName.uppercase())
        ps.executeQuery().use { rs -> return rs.next() }
    }
}

private fun generateDdlFromDb(conn: java.sql.Connection, tableName: String): String {
    val columns = mutableListOf<String>()
    val pk = mutableListOf<String>()

    // Get PK columns
    conn.prepareStatement("""
        SELECT cols.column_name FROM user_constraints cons
        JOIN user_cons_columns cols ON cons.constraint_name = cols.constraint_name
        WHERE cons.table_name = ? AND cons.constraint_type = 'P'
        ORDER BY cols.position
    """.trimIndent()).use { ps ->
        ps.setString(1, tableName.uppercase())
        ps.executeQuery().use { rs ->
            while (rs.next()) pk.add(rs.getString(1))
        }
    }

    // Get columns
    conn.prepareStatement("""
        SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
        FROM user_tab_columns WHERE table_name = ?
        ORDER BY column_id
    """.trimIndent()).use { ps ->
        ps.setString(1, tableName.uppercase())
        ps.executeQuery().use { rs ->
            while (rs.next()) {
                val name = rs.getString("COLUMN_NAME")
                val type = rs.getString("DATA_TYPE")
                val length = rs.getInt("DATA_LENGTH")
                val precision = rs.getObject("DATA_PRECISION") as? Number
                val scale = rs.getObject("DATA_SCALE") as? Number
                val nullable = rs.getString("NULLABLE")

                val typeStr = when {
                    type == "NUMBER" && precision != null && scale != null && scale.toInt() > 0 ->
                        "NUMBER(${precision},${scale})"
                    type == "NUMBER" && precision != null ->
                        "NUMBER(${precision})"
                    type == "NUMBER" -> "NUMBER"
                    type == "VARCHAR2" -> "VARCHAR2($length)"
                    type.startsWith("TIMESTAMP") -> "TIMESTAMP"
                    else -> type
                }

                val pkStr = if (name in pk) " PRIMARY KEY" else ""
                val nullStr = if (nullable == "N" && name !in pk) " NOT NULL" else ""
                columns.add("    $name${" ".repeat(maxOf(1, 30 - name.length))}$typeStr$pkStr$nullStr")
            }
        }
    }

    return "CREATE TABLE $tableName (\n${columns.joinToString(",\n")}\n);"
}

/**
 * Holds and persists the pipeline config (targets.json).
 */
class ConfigHolder(initial: PipelineConfig, private val configPath: String) {
    @Volatile
    var config: PipelineConfig = initial
        private set

    fun saveConfig(newConfig: PipelineConfig) {
        config = newConfig
        File(configPath).writeText(json.encodeToString(PipelineConfig.serializer(), newConfig))
        log.info("Config saved to {}", configPath)
    }

    companion object {
        private val log = LoggerFactory.getLogger(ConfigHolder::class.java)
        private val json = Json { prettyPrint = true }
    }
}
