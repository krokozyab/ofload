package app.migrations

import org.slf4j.LoggerFactory
import java.sql.Connection

/**
 * Minimal forward-only schema migrator for the system tables owned by this service.
 *
 * **Scope:** migrates only infrastructure tables (`ETL_WATERMARK`, `SCHEMA_MIGRATIONS`).
 * Per-target DDL (`STG_*`, `T_*`) is NOT migrated here — those remain manual under
 * `src/main/resources/ddl/` per project policy.
 *
 * **How it works:**
 *   1. On startup, [run] reads `/migrations/manifest.txt` (classpath resource) — one
 *      migration id per line, in dependency order.
 *   2. Ensures `SCHEMA_MIGRATIONS(ID VARCHAR2 PK, APPLIED_AT TIMESTAMP)` exists.
 *   3. Any id in the manifest but not in `SCHEMA_MIGRATIONS` is applied, in order.
 *   4. Each migration is a single SQL statement at `/migrations/<id>`. To support
 *      idempotence on pre-existing schemas, migrations typically wrap DDL in a
 *      PL/SQL block that swallows "already exists" errors (ORA-00955, ORA-01430).
 *
 * **Why self-made, not Flyway:** we need one tracking table and a handful of DDL
 * scripts. Flyway would be ~5 MB of dependency for ~50 lines of logic. The tradeoff
 * is that this runner has no checksum validation, no undo, no versioning sophistication;
 * that is fine for this project's scope.
 */
object Migrator {
    private val log = LoggerFactory.getLogger(Migrator::class.java)
    private const val TRACKING_TABLE = "SCHEMA_MIGRATIONS"
    private const val MANIFEST_PATH = "/migrations/manifest.txt"
    private const val MIGRATIONS_DIR = "/migrations"

    /**
     * Apply any pending migrations against [conn]. Called once at service startup
     * (see Main.kt) before the HTTP server comes up.
     *
     * The caller owns transaction control — this method uses `autoCommit = false`
     * during execution and issues its own `commit()` per migration, so each applied
     * migration is durable even if a later one fails.
     */
    fun run(conn: Connection) {
        val previousAutoCommit = conn.autoCommit
        conn.autoCommit = false
        try {
            ensureTrackingTable(conn)
            val applied = readApplied(conn)
            val manifest = loadManifest()
            val pending = manifest.filter { it !in applied }
            if (pending.isEmpty()) {
                log.info("Schema is up to date ({} migration(s) applied, 0 pending)", applied.size)
                return
            }
            log.info("Applying {} pending migration(s): {}", pending.size, pending)
            for (id in pending) {
                applyOne(conn, id)
            }
            log.info("Schema migrations complete.")
        } finally {
            conn.autoCommit = previousAutoCommit
        }
    }

    /**
     * Create the tracking table if it doesn't exist yet. First-run bootstrap only —
     * idempotent via USER_TABLES probe rather than ORA-00955 swallowing, since we
     * want a clean log message when the bootstrap actually runs.
     */
    private fun ensureTrackingTable(conn: Connection) {
        val exists = conn.prepareStatement(
            "SELECT 1 FROM user_tables WHERE table_name = ?"
        ).use { ps ->
            ps.setString(1, TRACKING_TABLE)
            ps.executeQuery().use { it.next() }
        }
        if (exists) return
        log.info("Bootstrapping tracking table {}", TRACKING_TABLE)
        conn.createStatement().use { st ->
            st.execute(
                """
                CREATE TABLE $TRACKING_TABLE (
                    ID         VARCHAR2(128) PRIMARY KEY,
                    APPLIED_AT TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
                )
                """.trimIndent()
            )
        }
        conn.commit()
    }

    /** Ids of migrations already recorded as applied. */
    private fun readApplied(conn: Connection): Set<String> {
        return conn.prepareStatement("SELECT ID FROM $TRACKING_TABLE").use { ps ->
            ps.executeQuery().use { rs ->
                val set = HashSet<String>()
                while (rs.next()) set.add(rs.getString(1))
                set
            }
        }
    }

    /**
     * Read the manifest: one migration id per line, `#`-prefixed comments and blank
     * lines ignored. The order in the manifest is the apply order — numeric prefixes
     * in file names are a human convention, not enforced here.
     */
    private fun loadManifest(): List<String> {
        val stream = Migrator::class.java.getResourceAsStream(MANIFEST_PATH)
            ?: error("$MANIFEST_PATH not found in classpath — migrations directory missing?")
        return stream.bufferedReader().use { reader ->
            reader.readLines()
                .map { it.trim() }
                .filter { it.isNotEmpty() && !it.startsWith("#") }
        }
    }

    /**
     * Load and execute a single migration, then insert its id into [TRACKING_TABLE].
     *
     * Each file holds exactly one SQL statement (typically a PL/SQL `BEGIN ... END;`
     * block that wraps the DDL with an exception handler for idempotence). Trailing
     * semicolons are trimmed — they are optional in JDBC and would produce
     * ORA-00911 if left on a bare DDL.
     */
    private fun applyOne(conn: Connection, id: String) {
        val path = "$MIGRATIONS_DIR/$id"
        val sql = Migrator::class.java.getResourceAsStream(path)
            ?.bufferedReader()?.use { it.readText() }
            ?: error("Migration file not found in classpath: $path")

        val trimmed = sql.trim().trimEnd(';')
        log.info("Applying migration {}", id)
        val startMs = System.currentTimeMillis()
        conn.createStatement().use { st -> st.execute(trimmed) }
        conn.prepareStatement("INSERT INTO $TRACKING_TABLE (ID) VALUES (?)").use { ps ->
            ps.setString(1, id)
            ps.executeUpdate()
        }
        conn.commit()
        log.info("Applied {} in {}ms", id, System.currentTimeMillis() - startMs)
    }
}
