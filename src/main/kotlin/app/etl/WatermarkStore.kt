package app.etl

import app.server.WatermarkRow
import org.slf4j.LoggerFactory
import java.sql.Connection

/**
 * CRUD on the ETL_WATERMARK table — the source of truth for incremental-load state.
 *
 * Each target has one row tracking: the last successfully processed watermark value,
 * current run status (RUNNING / OK / FAILED), run timestamps, rows loaded, and the
 * last error message. State transitions are driven by [Pipeline.runTarget] at well-
 * defined points so the UI can always report an accurate picture.
 */
object WatermarkStore {
    private val log = LoggerFactory.getLogger(WatermarkStore::class.java)

    /**
     * Mark any RUNNING targets older than [staleMinutes] as FAILED.
     *
     * Called once at service startup: if the previous JVM was killed mid-run, rows
     * are left in RUNNING forever and the /status endpoint would lie. This sweeps
     * them out so the UI reflects reality on the next page load. Only flips rows
     * that are demonstrably stale — an active run in another replica is safe.
     */
    fun resetStaleRunning(conn: Connection, staleMinutes: Int = 30) {
        val sql = """
            UPDATE ETL_WATERMARK SET
                LAST_STATUS = 'FAILED',
                LAST_RUN_FINISHED = SYSTIMESTAMP,
                ERROR_MESSAGE = 'Auto-reset: stale RUNNING state detected at service startup'
            WHERE LAST_STATUS = 'RUNNING'
              AND LAST_RUN_STARTED < SYSTIMESTAMP - INTERVAL '${staleMinutes}' MINUTE
        """.trimIndent()
        conn.prepareStatement(sql).use { ps ->
            val updated = ps.executeUpdate()
            if (updated > 0) {
                conn.commit()
                log.warn("Reset {} stale RUNNING target(s) (older than {}min)", updated, staleMinutes)
            }
        }
    }

    /**
     * Load every watermark row for the /status endpoint. Returns rows in table
     * order; callers that need lookup-by-name should re-index the result.
     */
    fun readAll(conn: Connection): List<WatermarkRow> {
        val sql = "SELECT TARGET_NAME, LAST_WM, LAST_STATUS, LAST_RUN_STARTED, LAST_RUN_FINISHED, ROWS_LOADED, ERROR_MESSAGE FROM ETL_WATERMARK"
        conn.prepareStatement(sql).use { ps ->
            ps.executeQuery().use { rs ->
                val results = mutableListOf<WatermarkRow>()
                while (rs.next()) {
                    results.add(WatermarkRow(
                        targetName = rs.getString("TARGET_NAME"),
                        lastWm = rs.getString("LAST_WM"),
                        lastStatus = rs.getString("LAST_STATUS"),
                        lastRunStarted = rs.getString("LAST_RUN_STARTED"),
                        lastRunFinished = rs.getString("LAST_RUN_FINISHED"),
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
     * the caller falls back to [TargetConfig.initialWm].
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
     * with current timestamp. Done via MERGE so the code path is the same for
     * brand-new and previously-seen targets, and so it's safe to re-run if the
     * caller decides to retry on transient DB errors.
     */
    fun beginRun(conn: Connection, targetName: String, wmColumn: String) {
        val sql = """
            MERGE INTO ETL_WATERMARK w
            USING (SELECT ? AS TN FROM DUAL) s ON (w.TARGET_NAME = s.TN)
            WHEN MATCHED THEN UPDATE SET
                LAST_STATUS = 'RUNNING', LAST_RUN_STARTED = SYSTIMESTAMP,
                WM_COLUMN = ?, ERROR_MESSAGE = NULL
            WHEN NOT MATCHED THEN INSERT
                (TARGET_NAME, WM_COLUMN, LAST_STATUS, LAST_RUN_STARTED)
                VALUES (?, ?, 'RUNNING', SYSTIMESTAMP)
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
     */
    fun finishRun(conn: Connection, targetName: String, newWm: String?, rowsLoaded: Long) {
        val sql = """
            UPDATE ETL_WATERMARK SET
                LAST_WM = ?, LAST_STATUS = 'OK',
                LAST_RUN_FINISHED = SYSTIMESTAMP, ROWS_LOADED = ?
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
     * own autocommit transaction.
     */
    fun failRun(conn: Connection, targetName: String, message: String) {
        val sql = """
            UPDATE ETL_WATERMARK SET
                LAST_STATUS = 'FAILED', LAST_RUN_FINISHED = SYSTIMESTAMP,
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
