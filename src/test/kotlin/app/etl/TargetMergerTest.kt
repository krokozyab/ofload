package app.etl

import app.config.TargetConfig
import kotlin.test.Test
import kotlin.test.assertEquals

class TargetMergerTest {
    @Test
    fun `merge uses raw targetMerge SQL from config`() {
        val mergeSql = """
            MERGE INTO T_ORDERS tgt USING STG_ORDERS src
            ON (tgt.ORDER_ID = src.ORDER_ID)
            WHEN MATCHED THEN UPDATE SET tgt.AMOUNT = src.AMOUNT
            WHEN NOT MATCHED THEN INSERT (ORDER_ID, AMOUNT) VALUES (src.ORDER_ID, src.AMOUNT)
        """.trimIndent()
        val cfg = TargetConfig(
            name = "orders", stagingTable = "STG_ORDERS", targetTable = "T_ORDERS",
            watermarkColumn = "UPDATED_AT", watermarkType = "TIMESTAMP",
            keyColumns = listOf("ORDER_ID"),
            sourceQuery = "SELECT * FROM ORDERS WHERE UPDATED_AT > ?",
            targetInsert = "INSERT INTO STG_ORDERS (ORDER_ID, AMOUNT) VALUES (?, ?)",
            targetMerge = mergeSql,
            initialWm = "1970-01-01T00:00:00"
        )
        assertEquals(mergeSql, cfg.targetMerge)
    }
}
