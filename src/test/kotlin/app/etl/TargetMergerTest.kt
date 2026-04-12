package app.etl

import app.config.TargetConfig
import kotlin.test.Test
import kotlin.test.assertContains

class TargetMergerTest {
    @Test
    fun `buildMergeSql generates correct MERGE`() {
        val cfg = TargetConfig(
            name = "orders", stagingTable = "STG_ORDERS", targetTable = "T_ORDERS",
            watermarkColumn = "UPDATED_AT", watermarkType = "TIMESTAMP",
            keyColumns = listOf("ORDER_ID"),
            columns = listOf("ORDER_ID", "CUSTOMER_ID", "AMOUNT", "UPDATED_AT"),
            sourceQuery = "SELECT * FROM ORDERS WHERE UPDATED_AT > ?",
            initialWm = "1970-01-01T00:00:00"
        )
        val sql = TargetMerger.buildMergeSql(cfg)
        assertContains(sql, "MERGE INTO T_ORDERS tgt")
        assertContains(sql, "USING STG_ORDERS src")
        assertContains(sql, "tgt.ORDER_ID = src.ORDER_ID")
        assertContains(sql, "WHEN MATCHED THEN UPDATE SET")
        assertContains(sql, "tgt.CUSTOMER_ID = src.CUSTOMER_ID")
        assertContains(sql, "WHEN NOT MATCHED THEN INSERT")
    }
}
