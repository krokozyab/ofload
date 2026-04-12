package app.etl

import app.config.TargetConfig
import java.sql.Connection

object TargetMerger {
    fun buildMergeSql(t: TargetConfig): String {
        val onClause = t.keyColumns.joinToString(" AND ") { "tgt.$it = src.$it" }
        val updateCols = t.columns.filter { it !in t.keyColumns }
        val updateSet = updateCols.joinToString(", ") { "tgt.$it = src.$it" }
        val insertCols = t.columns.joinToString(", ")
        val insertVals = t.columns.joinToString(", ") { "src.$it" }
        return """
            MERGE INTO ${t.targetTable} tgt
            USING ${t.stagingTable} src
            ON ($onClause)
            WHEN MATCHED THEN UPDATE SET $updateSet
            WHEN NOT MATCHED THEN INSERT ($insertCols) VALUES ($insertVals)
        """.trimIndent()
    }

    fun merge(conn: Connection, target: TargetConfig): Int =
        conn.createStatement().use { it.executeUpdate(buildMergeSql(target)) }
}
