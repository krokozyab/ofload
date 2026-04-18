package app.etl

import app.config.TargetConfig
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

/**
 * Unit tests for [StagingLoader.buildSourceQuery] and [StagingLoader.buildBindParams].
 * Both are pure functions — no DB access required.
 */
class StagingLoaderQueryTest {

    private fun target(
        name: String = "orders",
        keyColumns: List<String> = listOf("ORDER_ID"),
        pageSize: Int? = 10000,
        watermarkColumn: String = "UPDATED_AT",
        sourceQuery: String = "SELECT ORDER_ID, UPDATED_AT, AMOUNT FROM ORDERS WHERE /*WM*/"
    ) = TargetConfig(
        name = name,
        stagingTable = "STG_$name",
        targetTable = "T_$name",
        watermarkColumn = watermarkColumn,
        watermarkType = "TIMESTAMP",
        keyColumns = keyColumns,
        sourceQuery = sourceQuery,
        targetInsert = "INSERT INTO STG_$name (...) VALUES (?,?,?)",
        targetMerge = "MERGE ...",
        initialWm = "1970-01-01 00:00:00",
        pageSize = pageSize
    )

    // ── First-page (no lastKeyValues) ──────────────────────────────────

    @Test
    fun `first page single key uses scalar wm greater-than predicate`() {
        val sql = StagingLoader.buildSourceQuery(target(), lastKeyValues = null)
        assertContains(sql, "WHERE UPDATED_AT > ?")
        // Must be the bare `wm > ?` form, no seek-disjunction. Using `= ?` as a proxy
        // since it only appears in the tuple-seek branch, never in the first page.
        assertFalse(sql.contains("= ?"), "first page must not contain seek tuple branch")
    }

    @Test
    fun `first page with pagination appends ORDER BY wm and key`() {
        val sql = StagingLoader.buildSourceQuery(target(), lastKeyValues = null)
        assertContains(sql, "ORDER BY UPDATED_AT, ORDER_ID")
    }

    @Test
    fun `first page without pagination omits ORDER BY`() {
        val sql = StagingLoader.buildSourceQuery(target(pageSize = null), lastKeyValues = null)
        assertContains(sql, "WHERE UPDATED_AT > ?")
        assertFalse(sql.contains("ORDER BY"), "unpaginated query should not add ORDER BY")
    }

    // ── Seek-page (lastKeyValues supplied) ─────────────────────────────

    @Test
    fun `seek page with single key uses scalar form not tuple`() {
        val sql = StagingLoader.buildSourceQuery(target(), lastKeyValues = listOf("42"))
        assertContains(sql, "(UPDATED_AT > ? OR (UPDATED_AT = ? AND ORDER_ID > ?))")
        assertFalse(sql.contains("(ORDER_ID) > (?)"),
            "single-column key must use scalar form — ofjdbc/RP_ARB doesn't handle row-value expressions reliably")
    }

    @Test
    fun `seek page with composite key uses tuple form`() {
        val sql = StagingLoader.buildSourceQuery(
            target(keyColumns = listOf("HEADER_ID", "LINE_ID")),
            lastKeyValues = listOf("100", "5")
        )
        assertContains(sql,
            "(UPDATED_AT > ? OR (UPDATED_AT = ? AND (HEADER_ID, LINE_ID) > (?, ?)))")
    }

    @Test
    fun `seek page ORDER BY includes all key columns in order`() {
        val sql = StagingLoader.buildSourceQuery(
            target(keyColumns = listOf("A", "B", "C")),
            lastKeyValues = listOf("1", "2", "3")
        )
        assertContains(sql, "ORDER BY UPDATED_AT, A, B, C")
    }

    // ── Marker validation ──────────────────────────────────────────────

    @Test
    fun `missing WM marker triggers fail-fast with useful message`() {
        val badTarget = target(sourceQuery = "SELECT * FROM ORDERS WHERE UPDATED_AT > ?")
        val ex = assertFailsWith<IllegalArgumentException> {
            StagingLoader.buildSourceQuery(badTarget, lastKeyValues = null)
        }
        assertContains(ex.message!!, StagingLoader.WM_MARKER)
        assertContains(ex.message!!, "orders")
    }

    @Test
    fun `WM marker is replaced even when surrounded by other predicates`() {
        val t = target(sourceQuery = "SELECT * FROM X WHERE ORG_ID = 101 AND /*WM*/ AND STATUS = 'A'")
        val sql = StagingLoader.buildSourceQuery(t, lastKeyValues = null)
        assertContains(sql, "ORG_ID = 101 AND UPDATED_AT > ? AND STATUS = 'A'")
    }

    // ── Bind parameter assembly ────────────────────────────────────────

    @Test
    fun `first page binds only the watermark`() {
        val params = StagingLoader.buildBindParams(target(), "2026-04-17 10:00:00", lastKeyValues = null)
        assertEquals(listOf<String?>("2026-04-17 10:00:00"), params)
    }

    @Test
    fun `seek page binds wm twice plus each key once`() {
        val params = StagingLoader.buildBindParams(
            target(keyColumns = listOf("A", "B")),
            "2026-04-17 10:00:00",
            lastKeyValues = listOf("100", "200")
        )
        assertEquals(
            listOf<String?>("2026-04-17 10:00:00", "2026-04-17 10:00:00", "100", "200"),
            params
        )
    }

    @Test
    fun `unpaginated load binds only watermark even when lastKeyValues non-null`() {
        // Edge case: pageSize=null means we don't do tuple seek at all.
        val params = StagingLoader.buildBindParams(
            target(pageSize = null),
            "2026-04-17 10:00:00",
            lastKeyValues = listOf("ignored")
        )
        assertEquals(listOf<String?>("2026-04-17 10:00:00"), params)
    }

    @Test
    fun `seek page preserves null key values as nulls in bind list`() {
        // Not a realistic source — key columns are usually NOT NULL — but the bind
        // builder should pass nullables through unchanged rather than throwing.
        val params = StagingLoader.buildBindParams(
            target(keyColumns = listOf("A", "B")),
            "2026-04-17 10:00:00",
            lastKeyValues = listOf("100", null)
        )
        assertEquals(
            listOf<String?>("2026-04-17 10:00:00", "2026-04-17 10:00:00", "100", null),
            params
        )
    }
}
