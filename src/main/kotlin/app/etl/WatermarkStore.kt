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
 * Schema is split into three independent sections:
 *   * Progress cursor: `LAST_WM` (advances per-page within a run).
 *   * Current run:     `CURRENT_RUN_STARTED`, `CURRENT_RUN_ID` (NULL when idle).
 *   * Outcome history: `LAST_SUCCESS_*` and `LAST_FAILURE_*` (independent).
 *
 * State transitions are driven by [Pipeline.runTarget] at well-defined points:
 *   [beginRun] → N × [updateWatermark] → [finishRunOk]  — on the happy path
 *   [beginRun] → any point               → [failRun]    — on error
 *
 * This split removes the visual conflict the UI used to have where a RUNNING status
 * could sit next to stale LAST_RUN_FINISHED / ROWS_LOADED values from the previous run.
 *
 * Timezone policy: all timestamps are stored as UTC wall-clock via SYS_EXTRACT_UTC
 * and returned as ISO-8601 strings with a trailing 'Z'.
 */
object WatermarkStore {
    private val log = LoggerFactory.getLogger(WatermarkStore::class.java)
    private val UTC = TimeZone.getTimeZone("UTC")

    /**
     * Serialise a JDBC Timestamp read with the UTC calendar into an ISO-8601 string
     * (e.g. "2026-04-12T16:11:22.479Z"). [java.time.Instant.toString] gives a format
     * stable across JVMs and safe for `new Date(...)` on the JS side.
     */
    private fun Timestamp?.toIsoUtc(): String? = this?.toInstant()?.toString()

    /**
     * On service startup, flip every target still marked as "running" (CURRENT_RUN_*
     * populated) into a FAILED outcome. The service is single-instance, so any row
     * with CURRENT_RUN_STARTED set at boot is necessarily orphaned — the previous
     * process died (crash, OOM, SIGKILL, k8s restart) without transitioning it.
     * If this class is ever reused in a multi-instance deployment, add a distributed
     * lock / lease column before restoring any threshold logic.
     */
    fun resetStaleRunning(conn: Connection) {
        val sql = """
            UPDATE ETL_WATERMARK SET
                CURRENT_RUN_STARTED = NULL,
                CURRENT_RUN_ID = NULL,
                CURRENT_RUN_ROWS = 0,
                LAST_FAILURE_FINISHED = SYS_EXTRACT_UTC(SYSTIMESTAMP),
                LAST_FAILURE_ERROR = 'Auto-reset: RUNNING state at service startup (previous process terminated unexpectedly)'
            WHERE CURRENT_RUN_STARTED IS NOT NULL
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
     * as ISO-8601 strings ending in 'Z'.
     */
    fun readAll(conn: Connection): List<WatermarkRow> {
        val sql = """
            SELECT TARGET_NAME, LAST_WM,
                   CURRENT_RUN_STARTED, CURRENT_RUN_ID, CURRENT_RUN_ROWS,
                   LAST_SUCCESS_FINISHED, LAST_SUCCESS_ROWS, LAST_SUCCESS_DURATION_MS,
                   LAST_FAILURE_FINISHED, LAST_FAILURE_ERROR
            FROM ETL_WATERMARK
        """.trimIndent()
        conn.prepareStatement(sql).use { ps ->
            ps.executeQuery().use { rs ->
                val results = mutableListOf<WatermarkRow>()
                // Fresh Calendar per invocation (Calendar is not thread-safe and getTimestamp mutates it).
                val utcCal = Calendar.getInstance(UTC)
                while (rs.next()) {
                    val durationMs = rs.getLong("LAST_SUCCESS_DURATION_MS").takeIf { !rs.wasNull() }
                    results.add(WatermarkRow(
                        targetName = rs.getString("TARGET_NAME"),
                        lastWm = rs.getString("LAST_WM"),
                        currentRunStarted = rs.getTimestamp("CURRENT_RUN_STARTED", utcCal).toIsoUtc(),
                        currentRunId = rs.getString("CURRENT_RUN_ID"),
                        currentRunRows = rs.getLong("CURRENT_RUN_ROWS"),
                        lastSuccessFinished = rs.getTimestamp("LAST_SUCCESS_FINISHED", utcCal).toIsoUtc(),
                        lastSuccessRows = rs.getLong("LAST_SUCCESS_ROWS"),
                        lastSuccessDurationMs = durationMs,
                        lastFailureFinished = rs.getTimestamp("LAST_FAILURE_FINISHED", utcCal).toIsoUtc(),
                        lastFailureError = rs.getString("LAST_FAILURE_ERROR")
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
     * Mark a run as started.
     *
     * Sets `CURRENT_RUN_STARTED = now`, `CURRENT_RUN_ID = runId`. Updates `WM_COLUMN`
     * in case the target's watermark column changed in the config. Leaves the outcome
     * columns (`LAST_SUCCESS_*`, `LAST_FAILURE_*`) alone — they still represent the
     * previous run's outcome and the UI can show that context (e.g. "running, last
     * success 2h ago"). MERGE'd so brand-new targets get an INSERT and existing ones
     * get an UPDATE via the same call path.
     */
    fun beginRun(conn: Connection, targetName: String, wmColumn: String, runId: String) {
        val sql = """
            MERGE INTO ETL_WATERMARK w
            USING (SELECT ? AS TN FROM DUAL) s ON (w.TARGET_NAME = s.TN)
            WHEN MATCHED THEN UPDATE SET
                WM_COLUMN = ?,
                CURRENT_RUN_STARTED = SYS_EXTRACT_UTC(SYSTIMESTAMP),
                CURRENT_RUN_ID = ?,
                CURRENT_RUN_ROWS = 0
            WHEN NOT MATCHED THEN INSERT
                (TARGET_NAME, WM_COLUMN, CURRENT_RUN_STARTED, CURRENT_RUN_ID, CURRENT_RUN_ROWS)
                VALUES (?, ?, SYS_EXTRACT_UTC(SYSTIMESTAMP), ?, 0)
        """.trimIndent()
        conn.prepareStatement(sql).use { ps ->
            ps.setString(1, targetName)
            ps.setString(2, wmColumn)
            ps.setString(3, runId)
            ps.setString(4, targetName)
            ps.setString(5, wmColumn)
            ps.setString(6, runId)
            ps.executeUpdate()
        }
    }

    /**
     * Persist page progress — update LAST_WM and CURRENT_RUN_ROWS.
     *
     * Called after each successful page commit inside [Pipeline.runTarget]. Keeps the
     * resume cursor durable AND lets the dashboard show live row counts for the
     * in-flight run, without touching any completion-outcome fields (so a crash
     * between pages doesn't leave LAST_SUCCESS_* looking like a finished run).
     */
    fun updateProgress(conn: Connection, targetName: String, newWm: String?, totalRowsSoFar: Long) {
        val sql = """
            UPDATE ETL_WATERMARK SET
                LAST_WM = ?,
                CURRENT_RUN_ROWS = ?
            WHERE TARGET_NAME = ?
        """.trimIndent()
        conn.prepareStatement(sql).use { ps ->
            ps.setString(1, newWm)
            ps.setLong(2, totalRowsSoFar)
            ps.setString(3, targetName)
            ps.executeUpdate()
        }
    }

    /**
     * Record a successful run completion.
     *
     * Clears CURRENT_RUN_* (target returns to idle) and writes LAST_SUCCESS_FINISHED
     * + LAST_SUCCESS_ROWS. LAST_FAILURE_* is intentionally untouched — the outcome
     * columns are independent, so the UI can still show "failed 3 days ago, recovered
     * 2h ago" if it wants to render history.
     */
    fun finishRunOk(conn: Connection, targetName: String, rowsLoaded: Long, durationMs: Long) {
        val sql = """
            UPDATE ETL_WATERMARK SET
                CURRENT_RUN_STARTED = NULL,
                CURRENT_RUN_ID = NULL,
                CURRENT_RUN_ROWS = 0,
                LAST_SUCCESS_FINISHED = SYS_EXTRACT_UTC(SYSTIMESTAMP),
                LAST_SUCCESS_ROWS = ?,
                LAST_SUCCESS_DURATION_MS = ?
            WHERE TARGET_NAME = ?
        """.trimIndent()
        conn.prepareStatement(sql).use { ps ->
            ps.setLong(1, rowsLoaded)
            ps.setLong(2, durationMs)
            ps.setString(3, targetName)
            ps.executeUpdate()
        }
    }

    /**
     * Record a failed run. Clears CURRENT_RUN_* and writes LAST_FAILURE_FINISHED +
     * LAST_FAILURE_ERROR (truncated to fit VARCHAR2(4000)). Called from Pipeline's
     * catch block on a separate connection because the run's main connection may
     * already be poisoned, so this runs with its own autocommit transaction.
     */
    fun failRun(conn: Connection, targetName: String, message: String) {
        val sql = """
            UPDATE ETL_WATERMARK SET
                CURRENT_RUN_STARTED = NULL,
                CURRENT_RUN_ID = NULL,
                CURRENT_RUN_ROWS = 0,
                LAST_FAILURE_FINISHED = SYS_EXTRACT_UTC(SYSTIMESTAMP),
                LAST_FAILURE_ERROR = ?
            WHERE TARGET_NAME = ?
        """.trimIndent()
        conn.prepareStatement(sql).use { ps ->
            ps.setString(1, message.take(4000))
            ps.setString(2, targetName)
            ps.executeUpdate()
        }
    }
}
