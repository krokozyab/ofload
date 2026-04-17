package app.etl

import app.server.WatermarkRow
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.Timestamp
import java.util.Calendar
import java.util.TimeZone

/**
 * CRUD on the ETL_WATERMARK table — the source of truth for incremental-load state.
 *
 * Each target has one row tracking: the last successfully processed watermark value,
 * current run status (RUNNING / OK / FAILED), run timestamps, rows loaded, and the
 * last error message. State transitions are driven by [Pipeline.runTarget] at well-
 * defined points so the UI can always report an accurate picture.
 *
 * Timezone policy: all timestamps are stored as UTC wall-clock via SYS_EXTRACT_UTC
 * (the column type is plain TIMESTAMP, so any TZ info would be lost on write anyway)
 * and returned to callers as ISO-8601 strings with a trailing 'Z'. That keeps the
 * API unambiguous regardless of the DB session's NLS_TIMESTAMP_FORMAT or session TZ.
 */
object WatermarkStore {
    private val log = LoggerFactory.getLogger(WatermarkStore::class.java)
    private val UTC = TimeZone.getTimeZone("UTC")

    /**
     * Serialise a JDBC Timestamp read with the UTC calendar into an ISO-8601 string
     * (e.g. "2026-04-12T16:11:22.479Z"). Using [java.time.Instant.toString] keeps the
     * format stable across JVMs and safe for `new Date(...)` on the JS side.
     */
    private fun Timestamp?.toIsoUtc(): String? = this?.toInstant()?.toString()

    /**
     * Flip every RUNNING row to FAILED at service startup.
     *
     * The service is single-instance and owns run state only in this JVM's memory, so
     * any RUNNING row observed at startup is by definition orphaned — the previous
     * process died (crash, OOM, SIGKILL, k8s restart) without transitioning the row.
     * No time threshold: waiting 30 minutes after a crash-and-restart would block the
     * operator from re-submitting affected targets for that whole window.
     * If this class is ever reused in a multi-instance deployment, add a distributed
     * lock / lease column before re-introducing a stale threshold.
     */
    fun resetStaleRunning(conn: Connection) {
        val sql = """
            UPDATE ETL_WATERMARK SET
                LAST_STATUS = 'FAILED',
                LAST_RUN_FINISHED = SYS_EXTRACT_UTC(SYSTIMESTAMP),
                ERROR_MESSAGE = 'Auto-reset: RUNNING state at service startup (previous process terminated unexpectedly)'
            WHERE LAST_STATUS = 'RUNNING'
        """.trimIndent()
        conn.prepareStatement(sql).use { ps ->
            val updated = ps.executeUpdate()
            if (updated > 0) {
                conn.commit()
                log.warn("Reset {} orphaned RUNNING target(s) at startup", updated)
            }
        }
    }

    /**
     * Load every watermark row for the /status endpoint. Timestamps are read with an
     * explicit UTC calendar (so JDBC does not apply JVM-local TZ offset) and emitted
     * as ISO-8601 strings ending in 'Z' — see [toIsoUtc].
     */
    fun readAll(conn: Connection): List<WatermarkRow> {
        val sql = "SELECT TARGET_NAME, LAST_WM, LAST_STATUS, LAST_RUN_STARTED, LAST_RUN_FINISHED, ROWS_LOADED, ERROR_MESSAGE FROM ETL_WATERMARK"
        conn.prepareStatement(sql).use { ps ->
            ps.executeQuery().use { rs ->
                val results = mutableListOf<WatermarkRow>()
                // Fresh Calendar per invocation (Calendar is not thread-safe and getTimestamp mutates it).
                val utcCal = Calendar.getInstance(UTC)
                while (rs.next()) {
                    results.add(WatermarkRow(
                        targetName = rs.getString("TARGET_NAME"),
                        lastWm = rs.getString("LAST_WM"),
                        lastStatus = rs.getString("LAST_STATUS"),
                        lastRunStarted = rs.getTimestamp("LAST_RUN_STARTED", utcCal).toIsoUtc(),
                        lastRunFinished = rs.getTimestamp("LAST_RUN_FINISHED", utcCal).toIsoUtc(),
                        rowsLoaded = rs.getLong("ROWS_LOADED"),
                        errorMessage = rs.getString("ERROR_MESSAGE")
                    ))
                }
                return results
            }
        }
    }

    /**
     * Read only the LAST_WM value for one target — the sole input [Pipeline] needs
     * before starting a new run. Returns null when the target has never run yet;
     * the caller falls back to [app.config.TargetConfig.initialWm].
     */
    fun read(conn: Connection, targetName: String): String? {
        val sql = "SELECT LAST_WM FROM ETL_WATERMARK WHERE TARGET_NAME = ?"
        conn.prepareStatement(sql).use { ps ->
            ps.setString(1, targetName)
            ps.executeQuery().use { rs ->
                return if (rs.next()) rs.getString(1) else null
            }
        }
    }

    /**
     * Flip the target's row (or insert one on first-ever run) into RUNNING state
     * with the current UTC timestamp. Done via MERGE so the code path is the same for
     * brand-new and previously-seen targets, and so it's safe to re-run if the caller
     * decides to retry on transient DB errors.
     */
    fun beginRun(conn: Connection, targetName: String, wmColumn: String) {
        val sql = """
            MERGE INTO ETL_WATERMARK w
            USING (SELECT ? AS TN FROM DUAL) s ON (w.TARGET_NAME = s.TN)
            WHEN MATCHED THEN UPDATE SET
                LAST_STATUS = 'RUNNING', LAST_RUN_STARTED = SYS_EXTRACT_UTC(SYSTIMESTAMP),
                WM_COLUMN = ?, ERROR_MESSAGE = NULL
            WHEN NOT MATCHED THEN INSERT
                (TARGET_NAME, WM_COLUMN, LAST_STATUS, LAST_RUN_STARTED)
                VALUES (?, ?, 'RUNNING', SYS_EXTRACT_UTC(SYSTIMESTAMP))
        """.trimIndent()
        conn.prepareStatement(sql).use { ps ->
            ps.setString(1, targetName)
            ps.setString(2, wmColumn)
            ps.setString(3, targetName)
            ps.setString(4, wmColumn)
            ps.executeUpdate()
        }
    }

    /**
     * Record a successful run: persist the new watermark, flip status to OK, and
     * note row count. [Pipeline] calls this after each successful page so progress
     * is durable even if a later page fails (resume from where we left off on next run).
     * Timestamp stored as UTC wall-clock via SYS_EXTRACT_UTC.
     */
    fun finishRun(conn: Connection, targetName: String, newWm: String?, rowsLoaded: Long) {
        val sql = """
            UPDATE ETL_WATERMARK SET
                LAST_WM = ?, LAST_STATUS = 'OK',
                LAST_RUN_FINISHED = SYS_EXTRACT_UTC(SYSTIMESTAMP), ROWS_LOADED = ?
            WHERE TARGET_NAME = ?
        """.trimIndent()
        conn.prepareStatement(sql).use { ps ->
            ps.setString(1, newWm)
            ps.setLong(2, rowsLoaded)
            ps.setString(3, targetName)
            ps.executeUpdate()
        }
    }

    /**
     * Record a failed run with the error message truncated to fit ERROR_MESSAGE's
     * VARCHAR2(4000). Called from Pipeline's catch block on a separate connection
     * (the run's main connection may already be poisoned), so this runs with its
     * own autocommit transaction. Timestamp stored as UTC wall-clock.
     */
    fun failRun(conn: Connection, targetName: String, message: String) {
        val sql = """
            UPDATE ETL_WATERMARK SET
                LAST_STATUS = 'FAILED', LAST_RUN_FINISHED = SYS_EXTRACT_UTC(SYSTIMESTAMP),
                ERROR_MESSAGE = ?
            WHERE TARGET_NAME = ?
        """.trimIndent()
        conn.prepareStatement(sql).use { ps ->
            ps.setString(1, message.take(4000))
            ps.setString(2, targetName)
            ps.executeUpdate()
        }
    }
}
