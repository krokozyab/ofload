package app.server

import app.config.PipelineConfig
import app.config.TargetConfig
import app.db.OracleDs
import io.javalin.Javalin
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("app.server.TargetsApi")
private val json = Json { prettyPrint = true; ignoreUnknownKeys = true }

@Serializable
data class DdlResponse(
    val stagingDdl: String,
    val targetDdl: String,
    val stagingTableExists: Boolean,
    val targetTableExists: Boolean
)

fun configureTargetsApi(app: Javalin, configHolder: ConfigHolder) {

    // GET /api/targets — list all targets (read-only view of targets.json)
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

    // GET /api/targets/{name}/ddl — read-only DDL inspection from DB metadata
    app.get("/api/targets/{name}/ddl") { ctx ->
        val name = ctx.pathParam("name")
        val target = configHolder.config.targets.find { it.name == name }
        if (target == null) {
            ctx.status(404).result(json.encodeToString(ErrorResponse.serializer(), ErrorResponse("Target '$name' not found")))
            return@get
        }

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
}

private fun tableExists(conn: java.sql.Connection, tableName: String): Boolean {
    conn.prepareStatement("SELECT 1 FROM user_tables WHERE table_name = ?").use { ps ->
        ps.setString(1, tableName.uppercase())
        ps.executeQuery().use { rs -> return rs.next() }
    }
}

private fun generateDdlFromDb(conn: java.sql.Connection, tableName: String): String {
    val columns = mutableListOf<String>()
    val pk = mutableListOf<String>()

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
 * Read-only config holder. targets.json is loaded once at startup.
 */
class ConfigHolder(initial: PipelineConfig, @Suppress("unused") private val configPath: String) {
    val config: PipelineConfig = initial
}
