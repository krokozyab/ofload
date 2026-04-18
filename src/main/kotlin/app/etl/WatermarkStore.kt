package app.etl

import app.config.PipelineConfig
import app.config.TargetConfig
import app.server.WatermarkRow
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Calendar
import java.util.TimeZone

/**
 * CRUD on the ETL_WATERMARK table — the source of truth for incremental-load state.
 *
 * Schema is split into three independent sections:
 *   * Progress cursor: `LAST_WM_TIMESTAMP` (typed TIMESTAMP) or `LAST_WM_NUMBER`
 *     (typed NUMBER), chosen per target by [TargetConfig.watermarkType]. The legacy
 *     VARCHAR2 `LAST_WM` column is still in the schema but ignored by this store.
 *   * Current run:     `CURRENT_RUN_STARTED`, `CURRENT_RUN_ID`, `CURRENT_RUN_ROWS`
 *     (all NULL/0 when idle).
 *   * Outcome history: `LAST_SUCCESS_*` and `LAST_FAILURE_*` (independent).
 *
 * State transitions are driven by [Pipeline.runTarget] at well-defined points:
 *   [beginRun] → N × [updateProgress] → [finishRunOk]  — on the happy path
 *   [beginRun] → any point              → [failRun]    — on error
 *
 * Timezone policy: all run-lifecycle timestamps (CURRENT_RUN_STARTED,
 * LAST_SUCCESS_FINISHED, LAST_FAILURE_FINISHED) are stored as UTC wall-clock via
 * SYS_EXTRACT_UTC and returned as ISO-8601 strings with a trailing 'Z'. The target
 * watermark itself (`LAST_WM_TIMESTAMP`) is stored verbatim — it represents the
 * last source-side timestamp observed, in whatever timezone the source uses.
 */
object WatermarkStore {
    private val log = LoggerFactory.getLogger(WatermarkStore::class.java)
    private val UTC = TimeZone.getTimeZone("UTC")

    /** Canonical output format for TIMESTAMP watermarks (microsecond precision, matching Oracle TIMESTAMP(6)). */
    private val TS_OUTPUT_FMT: DateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")

    /**
     * Parse a TIMESTAMP watermark string coming from a source `SELECT MAX(wm) FROM STG`.
     * Accepts the common Oracle formats up to 9 fractional digits — varies slightly
     * depending on how the staging column and source NLS are configured.
     */
    private val TS_INPUT_FMT: DateTimeFormatter = DateTimeFormatter.ofPattern(
        "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS][.SSSSSSSS][.SSSSSSS][.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]"
    )

    /**
     * Serialise a JDBC Timestamp read with the UTC calendar into an ISO-8601 string
     * (e.g. "2026-04-12T16:11:22.479Z"). [java.time.Instant.toString] gives a format
     * stable across JVMs and safe for `new Date(...)` on the JS side.
     */
    private fun Timestamp?.toIsoUtc(): String? = this?.toInstant()?.toString()

    /**
     * Pick the column that holds this target's current watermark. Typed-per-target
     * so downstream SQL and reads stay correct even if different targets use
     * different watermark kinds.
     */
    private fun watermarkColumnName(type: String): String = when (type) {
        "TIMESTAMP" -> "LAST_WM_TIMESTAMP"
        "NUMBER" -> "LAST_WM_NUMBER"
        else -> error("Unsupported watermarkType: '$type' (expected 'TIMESTAMP' or 'NUMBER')")
    }

    /**
     * Bind a watermark string into a PreparedStatement using the right JDBC type.
     * For TIMESTAMP watermarks, parse the string with [TS_INPUT_FMT] first — fails
     * loudly if the source returned an unexpected format, which is better than the
     * old behaviour of silently storing it as VARCHAR.
     */
    private fun bindWatermark(ps: PreparedStatement, idx: Int, type: String, value: String?) {
        if (value == null) {
            ps.setNull(idx, if (type == "TIMESTAMP") Types.TIMESTAMP else Types.NUMERIC)
            return
        }
        when (type) {
            "TIMESTAMP" -> ps.setTimestamp(idx, Timestamp.valueOf(LocalDateTime.parse(value, TS_INPUT_FMT)))
            "NUMBER" -> ps.setBigDecimal(idx, value.toBigDecimal())
            else -> error("Unsupported watermarkType: '$type'")
        }
    }

    /**
     * Format a typed watermark value read from its column into the canonical string
     * form that [Pipeline] / [StagingLoader] bind back into source queries.
     */
    private fun readWatermark(
        type: String,
        tsReader: (String) -> Timestamp?,
        numReader: (String) -> java.math.BigDecimal?
    ): String? = when (type) {
        "TIMESTAMP" -> tsReader("LAST_WM_TIMESTAMP")?.toLocalDateTime()?.format(TS_OUTPUT_FMT)
        "NUMBER" -> numReader("LAST_WM_NUMBER")?.toPlainString()
        else -> null
    }

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
     * Load every watermark row for the /status endpoint. Timestamps in the outcome
     * columns are read with an explicit UTC calendar (so JDBC does not apply JVM-local
     * TZ offset) and emitted as ISO-8601 strings ending in 'Z'. The per-target
     * watermark is coalesced from either [LAST_WM_TIMESTAMP] or [LAST_WM_NUMBER]
     * depending on the target's declared `watermarkType` — passed in via [config].
     *
     * Rows whose target is not in the config (e.g. after a target was renamed) get
     * `lastWm = null` rather than erroring — the UI can still render the row.
     */
    fun readAll(conn: Connection, config: PipelineConfig): List<WatermarkRow> {
        val typeByName: Map<String, String> = config.targets.associate { it.name to it.watermarkType }
        val sql = """
            SELECT TARGET_NAME,
                   LAST_WM_TIMESTAMP, LAST_WM_NUMBER,
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
                    val name = rs.getString("TARGET_NAME")
                    val type = typeByName[name]
                    val lastWm = if (type != null) {
                        readWatermark(type,
                            tsReader = { rs.getTimestamp(it) },
                            numReader = { rs.getBigDecimal(it) })
                    } else null
                    val durationMs = rs.getLong("LAST_SUCCESS_DURATION_MS").takeIf { !rs.wasNull() }
                    results.add(WatermarkRow(
                        targetName = name,
                        lastWm = lastWm,
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
     * Read the stored watermark for one target in its canonical string form, ready to
     * be bound into the source SELECT. Returns null when the target has never run yet;
     * the caller falls back to [TargetConfig.initialWm].
     */
    fun read(conn: Connection, target: TargetConfig): String? {
        val col = watermarkColumnName(target.watermarkType)
        val sql = "SELECT $col FROM ETL_WATERMARK WHERE TARGET_NAME = ?"
        conn.prepareStatement(sql).use { ps ->
            ps.setString(1, target.name)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return when (target.watermarkType) {
                    "TIMESTAMP" -> rs.getTimestamp(1)?.toLocalDateTime()?.format(TS_OUTPUT_FMT)
                    "NUMBER" -> rs.getBigDecimal(1)?.toPlainString()
                    else -> null
                }
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
     * Persist page progress — update the typed watermark column (LAST_WM_TIMESTAMP or
     * LAST_WM_NUMBER, by [TargetConfig.watermarkType]) and CURRENT_RUN_ROWS.
     *
     * Called after each successful page commit inside [Pipeline.runTarget]. Keeps the
     * resume cursor durable AND lets the dashboard show live row counts for the
     * in-flight run, without touching any completion-outcome fields.
     *
     * The [newWm] value is validated at bind time — an unparseable string for a
     * TIMESTAMP target fails the update loudly rather than being silently stored.
     */
    fun updateProgress(conn: Connection, target: TargetConfig, newWm: String?, totalRowsSoFar: Long) {
        val col = watermarkColumnName(target.watermarkType)
        val sql = """
            UPDATE ETL_WATERMARK SET
                $col = ?,
                CURRENT_RUN_ROWS = ?
            WHERE TARGET_NAME = ?
        """.trimIndent()
        conn.prepareStatement(sql).use { ps ->
            bindWatermark(ps, 1, target.watermarkType, newWm)
            ps.setLong(2, totalRowsSoFar)
            ps.setString(3, target.name)
            ps.executeUpdate()
        }
    }

    /**
     * Record a successful run completion.
     *
     * Clears CURRENT_RUN_* (target returns to idle) and writes LAST_SUCCESS_FINISHED
     * + LAST_SUCCESS_ROWS + LAST_SUCCESS_DURATION_MS. LAST_FAILURE_* is intentionally
     * untouched — the outcome columns are independent, so the UI can still show
     * "failed 3 days ago, recovered 2h ago" if it wants to render history.
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
