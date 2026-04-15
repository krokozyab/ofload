package app.etl

import app.server.WatermarkRow
import org.slf4j.LoggerFactory
import java.sql.Connection

object WatermarkStore {
    private val log = LoggerFactory.getLogger(WatermarkStore::class.java)

    /**
     * Reset stale RUNNING targets — if RUNNING longer than [staleMinutes], mark as FAILED.
     * Called once at service startup.
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

    fun read(conn: Connection, targetName: String): String? {
        val sql = "SELECT LAST_WM FROM ETL_WATERMARK WHERE TARGET_NAME = ?"
        conn.prepareStatement(sql).use { ps ->
            ps.setString(1, targetName)
            ps.executeQuery().use { rs ->
                return if (rs.next()) rs.getString(1) else null
            }
        }
    }

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
