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

/**
 * Response body for GET /api/targets/{name}/ddl — the DDL actually present in the
 * database for a target's staging and final tables, reverse-engineered from Oracle
 * metadata views. `*TableExists` flags are returned separately so the UI can show
 * "not yet created" without parsing the DDL string.
 */
@Serializable
data class DdlResponse(
    val stagingDdl: String,
    val targetDdl: String,
    val stagingTableExists: Boolean,
    val targetTableExists: Boolean
)

/**
 * Wires the read-only /api/targets/... surface used by the targets visualisation page.
 *
 * All endpoints are GET — targets.json is hand-edited in the filesystem, not through
 * this API. The previous POST/PUT/DELETE endpoints that mutated config and schema
 * were removed to make targets.json the single source of truth.
 */
fun configureTargetsApi(app: Javalin, configHolder: ConfigHolder) {

    /** GET /api/targets — full config dump for the targets page. */
    app.get("/api/targets") { ctx ->
        ctx.status(200).result(json.encodeToString(PipelineConfig.serializer(), configHolder.config))
    }

    /** GET /api/targets/{name} — single target config. 404 on unknown name. */
    app.get("/api/targets/{name}") { ctx ->
        val name = ctx.pathParam("name")
        val target = configHolder.config.targets.find { it.name == name }
        if (target == null) {
            ctx.status(404).result(json.encodeToString(ErrorResponse.serializer(), ErrorResponse("Target '$name' not found")))
        } else {
            ctx.status(200).result(json.encodeToString(TargetConfig.serializer(), target))
        }
    }

    /**
     * GET /api/targets/{name}/ddl — read-only DDL inspection.
     *
     * Reverse-engineers current CREATE TABLE for both staging and target tables from
     * Oracle dictionary views, plus reports whether each table actually exists. Purely
     * informational — the UI uses this to show operators the live schema, not to apply
     * it. DB read failures degrade to placeholder DDL strings rather than 500ing the
     * whole endpoint, so the UI stays usable during transient DB issues.
     */
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

/**
 * Quick existence probe for a table by name. Uses user_tables rather than DESCRIBE
 * to avoid raising an exception on the happy path (missing tables are a normal state
 * when the operator hasn't applied DDL yet).
 */
private fun tableExists(conn: java.sql.Connection, tableName: String): Boolean {
    conn.prepareStatement("SELECT 1 FROM user_tables WHERE table_name = ?").use { ps ->
        ps.setString(1, tableName.uppercase())
        ps.executeQuery().use { rs -> return rs.next() }
    }
}

/**
 * Reverse-engineer a CREATE TABLE statement from Oracle dictionary views.
 *
 * Walks user_constraints/user_cons_columns to find the primary key, then
 * user_tab_columns for the column list, reformatting data types to readable SQL
 * (NUMBER(p), NUMBER(p,s), VARCHAR2(n), TIMESTAMP, etc.). The output is purely
 * informational — not executed anywhere — so the formatting trades full DDL fidelity
 * (no storage clauses, tablespaces, or indexes) for a compact, human-readable view.
 */
private fun generateDdlFromDb(conn: java.sql.Connection, tableName: String): String {
    val columns = mutableListOf<String>()
    val pk = mutableListOf<String>()

    // Find PK columns first so the column loop below can annotate them inline.
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

    // Column list in creation order, formatted like a hand-written CREATE TABLE.
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
 * Immutable in-memory holder of the parsed PipelineConfig.
 *
 * targets.json is loaded once at startup (see Main) from the classpath resource
 * and then only read — this class is a thin wrapper so the config reference can
 * be passed to components that need it (RunManager, the HTTP endpoints).
 */
class ConfigHolder(val config: PipelineConfig)
