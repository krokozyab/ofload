package app.config

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Unit tests for [TargetsValidator]. Pure static analysis — no DB required.
 */
class TargetsValidatorTest {

    private fun goodTarget(
        name: String = "orders",
        sourceQuery: String = "SELECT ORDER_ID, UPDATED_AT, AMOUNT FROM ORDERS WHERE /*WM*/",
        targetInsert: String = "INSERT /*+ APPEND_VALUES */ INTO STG_orders (ORDER_ID, UPDATED_AT, AMOUNT) VALUES (?, ?, ?)",
        targetMerge: String = "MERGE INTO T_orders tgt USING STG_orders src ON (tgt.ORDER_ID = src.ORDER_ID) WHEN MATCHED THEN UPDATE SET tgt.AMOUNT = src.AMOUNT WHEN NOT MATCHED THEN INSERT (ORDER_ID) VALUES (src.ORDER_ID)",
        keyColumns: List<String> = listOf("ORDER_ID"),
        watermarkColumn: String = "UPDATED_AT",
        watermarkType: String = "TIMESTAMP",
        initialWm: String = "1970-01-01 00:00:00",
        pageSize: Int? = 10000,
        lookbackMinutes: Long? = null
    ) = TargetConfig(
        name = name,
        stagingTable = "STG_$name",
        targetTable = "T_$name",
        watermarkColumn = watermarkColumn,
        watermarkType = watermarkType,
        keyColumns = keyColumns,
        sourceQuery = sourceQuery,
        targetInsert = targetInsert,
        targetMerge = targetMerge,
        initialWm = initialWm,
        pageSize = pageSize,
        lookbackMinutes = lookbackMinutes
    )

    private fun assertValid(target: TargetConfig) {
        val errors = TargetsValidator.validate(PipelineConfig(listOf(target)))
        assertTrue(errors.isEmpty(), "expected no errors, got: $errors")
    }

    private fun assertErrorContaining(target: TargetConfig, fragment: String) {
        val errors = TargetsValidator.validate(PipelineConfig(listOf(target)))
        assertTrue(
            errors.any { it.message.contains(fragment) },
            "expected an error containing '$fragment', got: ${errors.map { it.message }}"
        )
    }

    // ── Happy path ──────────────────────────────────────────────

    @Test fun `valid config passes validation`() = assertValid(goodTarget())

    @Test fun `valid composite-key config passes validation`() {
        val t = goodTarget(
            keyColumns = listOf("HEADER_ID", "LINE_ID"),
            sourceQuery = "SELECT HEADER_ID, LINE_ID, UPDATED_AT, AMOUNT FROM LINES WHERE /*WM*/",
            targetInsert = "INSERT INTO STG_orders (HEADER_ID, LINE_ID, UPDATED_AT, AMOUNT) VALUES (?, ?, ?, ?)"
        )
        assertValid(t)
    }

    @Test fun `NUMBER watermark with numeric initialWm passes`() {
        assertValid(goodTarget(watermarkType = "NUMBER", initialWm = "0"))
    }

    // ── Global rules ────────────────────────────────────────────

    @Test fun `duplicate target names produce a global error`() {
        val errors = TargetsValidator.validate(PipelineConfig(listOf(goodTarget("dup"), goodTarget("dup"))))
        assertTrue(errors.any { it.target == null && it.message.contains("duplicate target name") })
    }

    // ── sourceQuery ─────────────────────────────────────────────

    @Test fun `missing WM marker is flagged`() =
        assertErrorContaining(
            goodTarget(sourceQuery = "SELECT ORDER_ID, UPDATED_AT, AMOUNT FROM ORDERS WHERE UPDATED_AT > ?"),
            "/*WM*/"
        )

    @Test fun `non-SELECT sourceQuery is rejected`() =
        assertErrorContaining(
            goodTarget(sourceQuery = "DELETE FROM ORDERS WHERE /*WM*/"),
            "must begin with SELECT"
        )

    @Test fun `watermarkColumn absent from SELECT is flagged`() =
        assertErrorContaining(
            goodTarget(sourceQuery = "SELECT ORDER_ID, AMOUNT FROM ORDERS WHERE /*WM*/"),
            "watermarkColumn 'UPDATED_AT' is not in sourceQuery SELECT list"
        )

    @Test fun `keyColumn absent from SELECT is flagged`() =
        assertErrorContaining(
            goodTarget(sourceQuery = "SELECT UPDATED_AT, AMOUNT FROM ORDERS WHERE /*WM*/"),
            "keyColumn 'ORDER_ID' is not in sourceQuery SELECT list"
        )

    // ── targetInsert ────────────────────────────────────────────

    @Test fun `insert without placeholders is flagged`() {
        val t = goodTarget(
            targetInsert = "INSERT INTO STG_orders (ORDER_ID, UPDATED_AT, AMOUNT) VALUES (1, SYSTIMESTAMP, 0)"
        )
        assertErrorContaining(t, "placeholder count")
    }

    @Test fun `insert column-count mismatch with SELECT is flagged`() {
        val t = goodTarget(
            sourceQuery = "SELECT ORDER_ID, UPDATED_AT, AMOUNT FROM ORDERS WHERE /*WM*/",
            targetInsert = "INSERT INTO STG_orders (ORDER_ID, UPDATED_AT) VALUES (?, ?)"
        )
        assertErrorContaining(t, "does not match sourceQuery SELECT count")
    }

    @Test fun `insert not referencing staging table is flagged`() =
        assertErrorContaining(
            goodTarget(targetInsert = "INSERT INTO WRONG_TABLE (ORDER_ID, UPDATED_AT, AMOUNT) VALUES (?, ?, ?)"),
            "should reference stagingTable"
        )

    @Test fun `insert placeholder-vs-column-list mismatch is flagged`() {
        val t = goodTarget(
            targetInsert = "INSERT INTO STG_orders (ORDER_ID, UPDATED_AT, AMOUNT) VALUES (?, ?)"
        )
        assertErrorContaining(t, "placeholder count (2) does not match column list size (3)")
    }

    // ── targetMerge ─────────────────────────────────────────────

    @Test fun `merge missing staging table is flagged`() =
        assertErrorContaining(
            goodTarget(targetMerge = "MERGE INTO T_orders USING OTHER ON (1=1) WHEN MATCHED THEN UPDATE SET X=1"),
            "should reference stagingTable"
        )

    @Test fun `merge missing target table is flagged`() =
        assertErrorContaining(
            goodTarget(targetMerge = "MERGE INTO OTHER USING STG_orders ON (1=1) WHEN MATCHED THEN UPDATE SET X=1"),
            "should reference targetTable"
        )

    @Test fun `non-MERGE statement is flagged`() =
        assertErrorContaining(
            goodTarget(targetMerge = "UPDATE T_orders SET X = 1"),
            "must be a MERGE statement"
        )

    // ── Types / ranges ──────────────────────────────────────────

    @Test fun `invalid watermarkType is flagged`() =
        assertErrorContaining(
            goodTarget(watermarkType = "STRING"),
            "watermarkType must be 'TIMESTAMP' or 'NUMBER'"
        )

    @Test fun `malformed TIMESTAMP initialWm is flagged`() =
        assertErrorContaining(
            goodTarget(initialWm = "2026-04-17"),
            "does not parse as TIMESTAMP"
        )

    @Test fun `non-numeric initialWm for NUMBER watermark is flagged`() =
        assertErrorContaining(
            goodTarget(watermarkType = "NUMBER", initialWm = "not-a-number"),
            "is not a valid number"
        )

    @Test fun `empty keyColumns is flagged`() =
        assertErrorContaining(goodTarget(keyColumns = emptyList()), "keyColumns must contain at least one")

    @Test fun `zero pageSize is flagged`() =
        assertErrorContaining(goodTarget(pageSize = 0), "pageSize must be positive")

    @Test fun `negative lookbackMinutes is flagged`() =
        assertErrorContaining(goodTarget(lookbackMinutes = -10), "lookbackMinutes must be >= 0")

    // ── Parser helpers ──────────────────────────────────────────

    @Test fun `extractSelectColumns parses a typical query`() {
        val cols = TargetsValidator.extractSelectColumns(
            "SELECT A, B, C FROM T WHERE X > ?"
        )
        assertEquals(listOf("A", "B", "C"), cols)
    }

    @Test fun `extractSelectColumns returns null when FROM is missing`() {
        assertEquals(null, TargetsValidator.extractSelectColumns("SELECT 1"))
    }

    @Test fun `extractInsertColumns parses APPEND_VALUES hint`() {
        val cols = TargetsValidator.extractInsertColumns(
            "INSERT /*+ APPEND_VALUES */ INTO STG_T (A, B, C) VALUES (?, ?, ?)"
        )
        assertEquals(listOf("A", "B", "C"), cols)
    }

    @Test fun `extractInsertColumns returns null for VALUES-only INSERT`() {
        assertEquals(null, TargetsValidator.extractInsertColumns("INSERT INTO T VALUES (?, ?, ?)"))
    }

    // ── Aggregate ───────────────────────────────────────────────

    @Test fun `validator collects all errors in one pass`() {
        // Multiple problems in one target — all should be reported, not short-circuit.
        val t = goodTarget(
            sourceQuery = "SELECT AMOUNT FROM ORDERS WHERE X > ?",  // no /*WM*/, no ORDER_ID, no UPDATED_AT
            targetInsert = "INSERT INTO STG_orders (AMOUNT) VALUES (?)",  // mismatch
            watermarkType = "STRING"  // invalid
        )
        val errors = TargetsValidator.validate(PipelineConfig(listOf(t)))
        assertTrue(errors.size >= 4, "expected >=4 errors, got ${errors.size}: ${errors.map { it.message }}")
    }
}
