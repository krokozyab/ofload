package app.etl

import app.config.TargetConfig
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

data class LoadResult(val rowsLoaded: Long, val newWm: String?, val lastKeyValues: List<String?>?)

object StagingLoader {
    private val log = LoggerFactory.getLogger(StagingLoader::class.java)
    private const val BATCH_SIZE = 10000

    fun load(target: TargetConfig, sourceConn: Connection, oracleConn: Connection,
             lastWm: String?, lastKeyValues: List<String?>?): LoadResult {
        val t = target.name
        // 1. TRUNCATE staging
        log.info("[{}] Truncating {}", t, target.stagingTable)
        oracleConn.createStatement().use { it.execute("TRUNCATE TABLE ${target.stagingTable}") }

        // 2. Staging perf + optional NLS
        oracleConn.createStatement().use { st ->
            st.execute("ALTER TABLE ${target.stagingTable} NOLOGGING")
            target.nlsSettings?.forEach { (k, v) ->
                st.execute("ALTER SESSION SET $k = '$v'")
            }
        }

        // 3. Build query — apply lookback on first page only
        val rawWm = lastWm ?: target.initialWm
        val wm = if (target.lookbackMinutes != null && lastKeyValues == null && lastWm != null) {
            val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]")
            val adjusted = LocalDateTime.parse(rawWm, fmt).minusMinutes(target.lookbackMinutes)
            adjusted.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                .also { log.info("[{}] Lookback {}min: wm adjusted {} -> {}", t, target.lookbackMinutes, rawWm, it) }
        } else {
            rawWm
        }
        val keyColsCsv = target.keyColumns.joinToString(", ")
        val keyPlaceholders = target.keyColumns.joinToString(", ") { "?" }
        val query = if (target.pageSize != null) {
            if (lastKeyValues != null) {
                val baseQuery = target.sourceQuery.replace(
                    "${target.watermarkColumn} > ?",
                    "(${target.watermarkColumn} > ? OR (${target.watermarkColumn} = ? AND ($keyColsCsv) > ($keyPlaceholders)))"
                )
                "SELECT * FROM ($baseQuery ORDER BY ${target.watermarkColumn}, $keyColsCsv) WHERE ROWNUM <= ${target.pageSize}"
            } else {
                "SELECT * FROM (${target.sourceQuery} ORDER BY ${target.watermarkColumn}, $keyColsCsv) WHERE ROWNUM <= ${target.pageSize}"
            }
        } else {
            target.sourceQuery
        }

        // 4-6. Stream source → staging (no intermediate buffering)
        log.info("[{}] Streaming from source (wm={}, lastKeys={}, pageSize={})",
            t, wm, lastKeyValues, target.pageSize ?: "unlimited")

        val startStream = System.currentTimeMillis()
        var count = 0L
        var newLastKeys: List<String?>? = null

        sourceConn.prepareStatement(query).use { srcPs ->
            srcPs.fetchSize = BATCH_SIZE
            var paramIdx = 1
            srcPs.setString(paramIdx++, wm)
            if (target.pageSize != null && lastKeyValues != null) {
                srcPs.setString(paramIdx++, wm)
                for (v in lastKeyValues) srcPs.setString(paramIdx++, v)
            }
            srcPs.executeQuery().use { rs ->
                val colCount = rs.metaData.columnCount
                val keyIndices = target.keyColumns.map { rs.findColumn(it) }
                oracleConn.prepareStatement(target.targetInsert).use { insPs ->
                    while (rs.next()) {
                        for (i in 1..colCount) {
                            insPs.setObject(i, rs.getObject(i))
                        }
                        newLastKeys = keyIndices.map { rs.getObject(it)?.toString() }
                        insPs.addBatch()
                        count++
                        if (count % BATCH_SIZE == 0L) {
                            insPs.executeBatch()
                            oracleConn.commit()
                            log.info("[{}] Staged {} rows", t, count)
                        }
                    }
                    if (count % BATCH_SIZE != 0L) insPs.executeBatch()
                    oracleConn.commit()
                }
            }
        }
        val streamTime = System.currentTimeMillis() - startStream
        log.info("[{}] Source: {} rows read+staged in {}ms", t, count, streamTime)

        if (count == 0L) return LoadResult(0, lastWm, lastKeyValues)

        // 7. Get new watermark
        val newWm = oracleConn.createStatement().use { st ->
            st.executeQuery("SELECT MAX(${target.watermarkColumn}) FROM ${target.stagingTable}").use { wmRs ->
                if (wmRs.next()) wmRs.getString(1) else null
            }
        }
        return LoadResult(count, newWm ?: lastWm, newLastKeys)
    }
}
