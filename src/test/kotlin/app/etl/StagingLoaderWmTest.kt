package app.etl

import app.config.TargetConfig
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Unit tests for [StagingLoader.computeEffectiveWm] — the lookback logic that decides
 * whether to rewind the persisted watermark before the first page of a run.
 */
class StagingLoaderWmTest {

    private fun target(
        lookbackMinutes: Long? = null,
        initialWm: String = "1970-01-01 00:00:00"
    ) = TargetConfig(
        name = "t",
        stagingTable = "STG_T", targetTable = "T_T",
        watermarkColumn = "UPDATED_AT", watermarkType = "TIMESTAMP",
        keyColumns = listOf("ID"),
        sourceQuery = "SELECT * FROM T WHERE /*WM*/",
        targetInsert = "INSERT INTO STG_T VALUES (?)",
        targetMerge = "MERGE ...",
        initialWm = initialWm,
        pageSize = 10000,
        lookbackMinutes = lookbackMinutes
    )

    @Test
    fun `first-ever run falls back to initialWm`() {
        val wm = StagingLoader.computeEffectiveWm(
            target(initialWm = "2020-01-01 00:00:00"),
            lastWm = null,
            lastKeyValues = null
        )
        assertEquals("2020-01-01 00:00:00", wm)
    }

    @Test
    fun `run without lookback returns persisted wm unchanged`() {
        val wm = StagingLoader.computeEffectiveWm(
            target(lookbackMinutes = null),
            lastWm = "2026-04-17 10:00:00",
            lastKeyValues = null
        )
        assertEquals("2026-04-17 10:00:00", wm)
    }

    @Test
    fun `first page with lookback subtracts the configured minutes`() {
        val wm = StagingLoader.computeEffectiveWm(
            target(lookbackMinutes = 60),
            lastWm = "2026-04-17 10:00:00",
            lastKeyValues = null
        )
        assertEquals("2026-04-17 09:00:00", wm)
    }

    @Test
    fun `lookback does not cross midnight boundary incorrectly`() {
        // 5-minute lookback from 00:02 → yesterday 23:57. Regression guard.
        val wm = StagingLoader.computeEffectiveWm(
            target(lookbackMinutes = 5),
            lastWm = "2026-04-17 00:02:00",
            lastKeyValues = null
        )
        assertEquals("2026-04-16 23:57:00", wm)
    }

    @Test
    fun `seek page skips lookback even when configured`() {
        // On page 2+ within a run, lookback must NOT re-apply — we've already scanned
        // the lookback window on page 1. If it applied we'd cycle over the same rows.
        val wm = StagingLoader.computeEffectiveWm(
            target(lookbackMinutes = 60),
            lastWm = "2026-04-17 10:00:00",
            lastKeyValues = listOf("42")
        )
        assertEquals("2026-04-17 10:00:00", wm)
    }

    @Test
    fun `first-ever run with lookback ignores lookback since lastWm is null`() {
        val wm = StagingLoader.computeEffectiveWm(
            target(lookbackMinutes = 60),
            lastWm = null,
            lastKeyValues = null
        )
        assertEquals("1970-01-01 00:00:00", wm)
    }

    @Test
    fun `high-precision timestamp parses correctly`() {
        // Oracle TIMESTAMP(6) format: yyyy-MM-dd HH:mm:ss.SSSSSS
        val wm = StagingLoader.computeEffectiveWm(
            target(lookbackMinutes = 30),
            lastWm = "2026-04-17 10:30:45.123456",
            lastKeyValues = null
        )
        assertEquals("2026-04-17 10:00:45", wm)
    }

    @Test
    fun `lookback zero is a no-op`() {
        val wm = StagingLoader.computeEffectiveWm(
            target(lookbackMinutes = 0),
            lastWm = "2026-04-17 10:00:00",
            lastKeyValues = null
        )
        assertEquals("2026-04-17 10:00:00", wm)
    }
}
