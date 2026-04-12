package app.etl

import app.config.TargetConfig
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.sql.Connection
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

data class LoadResult(val rowsLoaded: Long, val newWm: String?, val lastKeyValue: String?)

object StagingLoader {
    private val log = LoggerFactory.getLogger(StagingLoader::class.java)
    private const val BATCH_SIZE = 10000

    fun load(target: TargetConfig, sourceConn: Connection, oracleConn: Connection,
             lastWm: String?, lastKeyValue: String?): LoadResult {
        val t = target.name
        // 1. TRUNCATE staging
        log.info("[{}] Truncating {}", t, target.stagingTable)
        oracleConn.createStatement().use { it.execute("TRUNCATE TABLE ${target.stagingTable}") }

        // 2. NLS + performance
        oracleConn.createStatement().use { st ->
            st.execute("ALTER TABLE ${target.stagingTable} NOLOGGING")
            st.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")
            st.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'")
            st.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
        }

        // 3. Build query — apply lookback on first page only
        val rawWm = lastWm ?: target.initialWm
        val wm = if (target.lookbackMinutes != null && lastKeyValue == null && lastWm != null) {
            val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]")
            val adjusted = LocalDateTime.parse(rawWm, fmt).minusMinutes(target.lookbackMinutes)
            adjusted.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                .also { log.info("[{}] Lookback {}min: wm adjusted {} -> {}", t, target.lookbackMinutes, rawWm, it) }
        } else {
            rawWm
        }
        val keyCol = target.keyColumns[0]
        val query = if (target.pageSize != null) {
            if (lastKeyValue != null) {
                val baseQuery = target.sourceQuery.replace(
                    "${target.watermarkColumn} > ?",
                    "(${target.watermarkColumn} > ? OR (${target.watermarkColumn} = ? AND $keyCol > ?))"
                )
                "SELECT * FROM ($baseQuery ORDER BY ${target.watermarkColumn}, $keyCol) WHERE ROWNUM <= ${target.pageSize}"
            } else {
                "SELECT * FROM (${target.sourceQuery} ORDER BY ${target.watermarkColumn}, $keyCol) WHERE ROWNUM <= ${target.pageSize}"
            }
        } else {
            target.sourceQuery
        }

        // 4. Read from source
        log.info("[{}] Reading from source (wm={}, lastKey={}, pageSize={})", t, wm, lastKeyValue, target.pageSize ?: "unlimited")
        val rows = mutableListOf<Array<Any?>>()
        val keyColIdx = target.columns.indexOf(keyCol)
        val startRead = System.currentTimeMillis()
        sourceConn.prepareStatement(query).use { srcPs ->
            var paramIdx = 1
            srcPs.setString(paramIdx++, wm)
            if (target.pageSize != null && lastKeyValue != null) {
                srcPs.setString(paramIdx++, wm)
                srcPs.setString(paramIdx++, lastKeyValue)
            }
            srcPs.executeQuery().use { rs ->
                val colCount = target.columns.size
                while (rs.next()) {
                    val row = Array<Any?>(colCount) { i -> rs.getObject(i + 1) }
                    rows.add(row)
                }
            }
        }
        val readTime = System.currentTimeMillis() - startRead
        log.info("[{}] Source: {} rows read in {}ms", t, rows.size, readTime)

        if (rows.isEmpty()) return LoadResult(0, lastWm, lastKeyValue)

        // 5. Capture last key value
        val newLastKey = rows.last()[keyColIdx]?.toString()

        // 6. Insert into staging
        log.info("[{}] Inserting {} rows into {}", t, rows.size, target.stagingTable)
        val placeholders = target.columns.joinToString(", ") { "?" }
        val insertSql = "INSERT /*+ APPEND_VALUES */ INTO ${target.stagingTable} (${target.columns.joinToString(", ")}) VALUES ($placeholders)"
        val startInsert = System.currentTimeMillis()
        oracleConn.prepareStatement(insertSql).use { insPs ->
            var inserted = 0L
            for (row in rows) {
                for (i in row.indices) {
                    insPs.setObject(i + 1, row[i])
                }
                insPs.addBatch()
                inserted++
                if (inserted % BATCH_SIZE == 0L) {
                    insPs.executeBatch()
                    oracleConn.commit()
                    log.info("[{}] Inserted {} / {} rows", t, inserted, rows.size)
                }
            }
            if (inserted % BATCH_SIZE != 0L) insPs.executeBatch()
            oracleConn.commit()
        }
        val insertTime = System.currentTimeMillis() - startInsert
        log.info("[{}] Inserted {} rows in {}ms", t, rows.size, insertTime)

        // 7. Get new watermark
        val newWm = oracleConn.createStatement().use { st ->
            st.executeQuery("SELECT MAX(${target.watermarkColumn}) FROM ${target.stagingTable}").use { wmRs ->
                if (wmRs.next()) wmRs.getString(1) else null
            }
        }
        return LoadResult(rows.size.toLong(), newWm ?: lastWm, newLastKey)
    }
}
