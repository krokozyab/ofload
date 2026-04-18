package app.etl

import app.config.PipelineConfig
import app.config.TargetConfig
import app.db.OracleDs
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.sql.Connection
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Integration tests for [WatermarkStore] against a real Oracle ATP instance.
 *
 * Runs against the same UCP pool / credentials as the rest of the app, reusing
 * whatever [OracleDs] picks up from `DB_USER` / `DB_PASSWORD` / `DB_CONNECT_STRING`
 * / `TNS_ADMIN`. The suite is **skipped automatically** when those env vars are
 * missing so that `./gradlew test` still passes on a fresh checkout without
 * credentials.
 *
 * Isolation: every test uses a target name prefixed with `__IT__` and cleans up
 * its rows before and after, so the suite never interferes with real pipeline
 * watermarks. The ETL_WATERMARK table itself must already exist (migrations 001 + 002 applied).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WatermarkStoreIntegrationTest {

    companion object {
        private const val TARGET_PREFIX = "__IT__"
        private fun credentialsPresent(): Boolean =
            !System.getenv("DB_USER").isNullOrBlank() &&
            !System.getenv("DB_PASSWORD").isNullOrBlank() &&
            !System.getenv("DB_CONNECT_STRING").isNullOrBlank()

        /** Factory for a minimal [TargetConfig] — only the fields WatermarkStore reads are meaningful. */
        private fun testTarget(
            name: String,
            watermarkType: String = "TIMESTAMP"
        ): TargetConfig = TargetConfig(
            name = name,
            stagingTable = "STG_NONE",
            targetTable = "T_NONE",
            watermarkColumn = "UPDATED_AT",
            watermarkType = watermarkType,
            keyColumns = listOf("ID"),
            sourceQuery = "SELECT * FROM T WHERE /*WM*/",
            targetInsert = "INSERT INTO STG_NONE VALUES (?)",
            targetMerge = "MERGE ...",
            initialWm = if (watermarkType == "TIMESTAMP") "1970-01-01 00:00:00" else "0",
            pageSize = 10000
        )
    }

    @BeforeAll
    fun checkEnvironment() {
        assumeTrue(
            credentialsPresent(),
            "Skipping DB integration tests — DB_USER/DB_PASSWORD/DB_CONNECT_STRING not set"
        )
    }

    /** Wipe any __IT__* rows left over from a crashed prior run. */
    private fun cleanup(conn: Connection) {
        conn.prepareStatement("DELETE FROM ETL_WATERMARK WHERE TARGET_NAME LIKE ?").use { ps ->
            ps.setString(1, "$TARGET_PREFIX%")
            ps.executeUpdate()
        }
        conn.commit()
    }

    /** Convenience: wrap a list of targets into a PipelineConfig for readAll(). */
    private fun pipelineConfig(vararg targets: TargetConfig): PipelineConfig =
        PipelineConfig(targets.toList())

    @Test
    fun `full happy-path lifecycle — beginRun, updateProgress, finishRunOk`() {
        val target = testTarget("${TARGET_PREFIX}happy")
        val config = pipelineConfig(target)
        OracleDs.getConnection().use { conn ->
            conn.autoCommit = false
            cleanup(conn)

            // Before first run — target has no row yet.
            assertNull(WatermarkStore.read(conn, target))

            // beginRun creates the row, sets CURRENT_RUN_*, leaves outcomes unpopulated.
            WatermarkStore.beginRun(conn, target.name, target.watermarkColumn, "run-1")
            conn.commit()
            val rowAfterBegin = WatermarkStore.readAll(conn, config).single { it.targetName == target.name }
            assertNotNull(rowAfterBegin.currentRunStarted, "CURRENT_RUN_STARTED must be set")
            assertEquals("run-1", rowAfterBegin.currentRunId)
            assertEquals(0L, rowAfterBegin.currentRunRows)
            assertNull(rowAfterBegin.lastSuccessFinished)

            // Per-page progress (typed watermark path).
            WatermarkStore.updateProgress(conn, target, "2026-04-17 10:00:00", 5000)
            conn.commit()
            val rowMidRun = WatermarkStore.readAll(conn, config).single { it.targetName == target.name }
            // Read round-trip — store persists as TIMESTAMP(6) and emits canonical format with .SSSSSS.
            assertEquals("2026-04-17 10:00:00.000000", rowMidRun.lastWm)
            assertEquals(5000L, rowMidRun.currentRunRows)
            assertNull(rowMidRun.lastSuccessFinished)

            // Successful completion.
            WatermarkStore.finishRunOk(conn, target.name, rowsLoaded = 15000, durationMs = 42_000)
            conn.commit()
            val rowDone = WatermarkStore.readAll(conn, config).single { it.targetName == target.name }
            assertNull(rowDone.currentRunStarted)
            assertNull(rowDone.currentRunId)
            assertEquals(0L, rowDone.currentRunRows)
            assertNotNull(rowDone.lastSuccessFinished)
            assertEquals(15000L, rowDone.lastSuccessRows)
            assertEquals(42_000L, rowDone.lastSuccessDurationMs)
            // WM persisted from updateProgress — finishRunOk does not overwrite it.
            assertEquals("2026-04-17 10:00:00.000000", rowDone.lastWm)

            cleanup(conn)
        }
    }

    @Test
    fun `TIMESTAMP watermark round-trips with microsecond precision`() {
        val target = testTarget("${TARGET_PREFIX}tsprec")
        val config = pipelineConfig(target)
        OracleDs.getConnection().use { conn ->
            conn.autoCommit = false
            cleanup(conn)

            WatermarkStore.beginRun(conn, target.name, target.watermarkColumn, "r")
            WatermarkStore.updateProgress(conn, target, "2026-04-17 14:47:26.634828", 1)
            conn.commit()

            assertEquals("2026-04-17 14:47:26.634828", WatermarkStore.read(conn, target))
            assertEquals(
                "2026-04-17 14:47:26.634828",
                WatermarkStore.readAll(conn, config).single { it.targetName == target.name }.lastWm
            )

            cleanup(conn)
        }
    }

    @Test
    fun `NUMBER watermark stored and read via LAST_WM_NUMBER`() {
        val target = testTarget("${TARGET_PREFIX}numwm", watermarkType = "NUMBER")
        val config = pipelineConfig(target)
        OracleDs.getConnection().use { conn ->
            conn.autoCommit = false
            cleanup(conn)

            WatermarkStore.beginRun(conn, target.name, target.watermarkColumn, "r")
            WatermarkStore.updateProgress(conn, target, "12345678901234", 100)
            conn.commit()

            assertEquals("12345678901234", WatermarkStore.read(conn, target))

            cleanup(conn)
        }
    }

    @Test
    fun `failed run clears CURRENT_RUN and writes LAST_FAILURE`() {
        val target = testTarget("${TARGET_PREFIX}fail")
        val config = pipelineConfig(target)
        OracleDs.getConnection().use { conn ->
            conn.autoCommit = false
            cleanup(conn)

            WatermarkStore.beginRun(conn, target.name, target.watermarkColumn, "run-x")
            conn.commit()
            WatermarkStore.failRun(conn, target.name, "Simulated source timeout")
            conn.commit()

            val row = WatermarkStore.readAll(conn, config).single { it.targetName == target.name }
            assertNull(row.currentRunStarted)
            assertNull(row.currentRunId)
            assertEquals(0L, row.currentRunRows)
            assertNotNull(row.lastFailureFinished)
            assertEquals("Simulated source timeout", row.lastFailureError)
            assertNull(row.lastSuccessFinished)

            cleanup(conn)
        }
    }

    @Test
    fun `beginRun after a failure clears previous error message`() {
        val target = testTarget("${TARGET_PREFIX}retry")
        val config = pipelineConfig(target)
        OracleDs.getConnection().use { conn ->
            conn.autoCommit = false
            cleanup(conn)

            WatermarkStore.beginRun(conn, target.name, target.watermarkColumn, "r1")
            WatermarkStore.failRun(conn, target.name, "boom")
            conn.commit()
            WatermarkStore.beginRun(conn, target.name, target.watermarkColumn, "r2")
            conn.commit()

            val row = WatermarkStore.readAll(conn, config).single { it.targetName == target.name }
            assertNotNull(row.currentRunStarted)
            assertEquals("r2", row.currentRunId)
            // Previous failure remains as history — only current-run columns are reset.
            assertNotNull(row.lastFailureFinished)
            assertEquals("boom", row.lastFailureError)

            cleanup(conn)
        }
    }

    @Test
    fun `finishRunOk leaves last-failure history intact`() {
        val target = testTarget("${TARGET_PREFIX}mixed")
        val config = pipelineConfig(target)
        OracleDs.getConnection().use { conn ->
            conn.autoCommit = false
            cleanup(conn)

            WatermarkStore.beginRun(conn, target.name, target.watermarkColumn, "r1")
            WatermarkStore.failRun(conn, target.name, "day-1 failure")
            conn.commit()
            WatermarkStore.beginRun(conn, target.name, target.watermarkColumn, "r2")
            WatermarkStore.updateProgress(conn, target, "2026-04-17 10:00:00", 100)
            WatermarkStore.finishRunOk(conn, target.name, rowsLoaded = 100, durationMs = 1000)
            conn.commit()

            val row = WatermarkStore.readAll(conn, config).single { it.targetName == target.name }
            assertNotNull(row.lastSuccessFinished)
            assertEquals(100L, row.lastSuccessRows)
            assertNotNull(row.lastFailureFinished)
            assertEquals("day-1 failure", row.lastFailureError)

            cleanup(conn)
        }
    }

    @Test
    fun `resetStaleRunning converts orphaned CURRENT_RUN rows to LAST_FAILURE`() {
        val target = testTarget("${TARGET_PREFIX}stale")
        val config = pipelineConfig(target)
        OracleDs.getConnection().use { conn ->
            conn.autoCommit = false
            cleanup(conn)

            WatermarkStore.beginRun(conn, target.name, target.watermarkColumn, "r-dead")
            conn.commit()

            WatermarkStore.resetStaleRunning(conn)

            val row = WatermarkStore.readAll(conn, config).single { it.targetName == target.name }
            assertNull(row.currentRunStarted, "CURRENT_RUN_STARTED must be cleared")
            assertNull(row.currentRunId)
            assertEquals(0L, row.currentRunRows)
            assertNotNull(row.lastFailureFinished)
            assertNotNull(row.lastFailureError)
            assertTrue(row.lastFailureError!!.contains("Auto-reset"),
                "error message should identify the auto-reset source")

            cleanup(conn)
        }
    }

    @Test
    fun `read returns only the watermark for a single target`() {
        val t1 = testTarget("${TARGET_PREFIX}r1")
        val t2 = testTarget("${TARGET_PREFIX}r2")
        OracleDs.getConnection().use { conn ->
            conn.autoCommit = false
            cleanup(conn)

            WatermarkStore.beginRun(conn, t1.name, t1.watermarkColumn, "x")
            WatermarkStore.updateProgress(conn, t1, "2026-04-17 10:00:00", 10)
            WatermarkStore.beginRun(conn, t2.name, t2.watermarkColumn, "y")
            WatermarkStore.updateProgress(conn, t2, "2026-04-17 11:00:00", 20)
            conn.commit()

            assertEquals("2026-04-17 10:00:00.000000", WatermarkStore.read(conn, t1))
            assertEquals("2026-04-17 11:00:00.000000", WatermarkStore.read(conn, t2))
            assertNotEquals(WatermarkStore.read(conn, t1), WatermarkStore.read(conn, t2))

            cleanup(conn)
        }
    }
}
