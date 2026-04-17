package app.etl

import app.config.TargetConfig
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Outcome of a single [StagingLoader.load] call — one page of the incremental load.
 *
 * @property rowsLoaded number of rows written to staging on this page. Zero signals
 *   "no more data, stop paginating" to [Pipeline].
 * @property newWm highest watermark observed on this page (MAX(wm) from staging),
 *   or the prior watermark when the page was empty.
 * @property lastKeyValues last row's key-column values (in keyColumns order),
 *   used to seed the next page's tuple pagination. Null on empty page.
 */
data class LoadResult(val rowsLoaded: Long, val newWm: String?, val lastKeyValues: List<String?>?)

/**
 * Streams one page of rows from Fusion source into the ATP staging table.
 *
 * Responsibilities per call:
 *  1. Truncate the staging table (per-target STG_* — safe, independent of other targets).
 *  2. Apply optional NLS session settings so typed staging columns accept source strings.
 *  3. Build a tuple-paginated query: `ORDER BY (watermarkColumn, key1, key2, ...)`
 *     with a composite-key WHERE seek so repeated pages resume exactly where the last ended,
 *     even when multiple rows share a watermark value.
 *  4. Stream rows directly from source ResultSet into the staging PreparedStatement
 *     with [BATCH_SIZE]-sized commits — no intermediate List, so memory stays flat on
 *     tables with millions of rows.
 *  5. Capture the new high-water mark for the next run.
 *
 * The INSERT SQL comes verbatim from [TargetConfig.targetInsert] — this loader performs
 * no SQL generation, keeping the behaviour fully inspectable in targets.json.
 */
object StagingLoader {
    private val log = LoggerFactory.getLogger(StagingLoader::class.java)

    /**
     * Commit cadence for both source fetchSize and staging executeBatch.
     * 10k is a compromise: big enough to amortise round-trips, small enough that a
     * single failed batch doesn't roll back a huge amount of progress.
     */
    private const val BATCH_SIZE = 10000

    /**
     * Load one page of data from source into staging.
     *
     * @param target configuration describing source query, staging table and keys.
     * @param sourceConn open Fusion JDBC connection (from [app.db.SourceDs] or a shared holder).
     * @param oracleConn open ATP connection with autoCommit=false — the caller is expected
     *   to manage the surrounding transaction.
     * @param lastWm persisted watermark from prior successful runs, or null on first-ever run.
     * @param lastKeyValues key-column values of the last row from the previous page (within
     *   the same run). Null on the first page — then the WHERE clause uses only `wm > ?`.
     */
    fun load(target: TargetConfig, sourceConn: Connection, oracleConn: Connection,
             lastWm: String?, lastKeyValues: List<String?>?): LoadResult {
        val t = target.name
        // 1. TRUNCATE staging — clears the slate so this page's data is the only
        //    content MERGE will see. DDL commits implicitly, which is fine since
        //    staging has no cross-run state we need to preserve.
        log.info("[{}] Truncating {}", t, target.stagingTable)
        oracleConn.createStatement().use { it.execute("TRUNCATE TABLE ${target.stagingTable}") }

        // 2. NOLOGGING on staging (redo is wasted for a truncated-every-run table)
        //    plus optional per-target NLS settings — needed when staging columns are typed
        //    (DATE/NUMBER) and the source driver delivers strings.
        oracleConn.createStatement().use { st ->
            st.execute("ALTER TABLE ${target.stagingTable} NOLOGGING")
            target.nlsSettings?.forEach { (k, v) ->
                st.execute("ALTER SESSION SET $k = '$v'")
            }
        }

        // 3. Resolve the effective watermark for this page.
        //    On the very first page of a run, apply optional lookbackMinutes — subtract
        //    that many minutes from the persisted watermark so we re-scan recently-updated
        //    rows (guards against source-side clock skew and late-arriving updates).
        //    Subsequent pages of the same run use the raw value — we've already scanned.
        val rawWm = lastWm ?: target.initialWm
        val wm = if (target.lookbackMinutes != null && lastKeyValues == null && lastWm != null) {
            val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]")
            val adjusted = LocalDateTime.parse(rawWm, fmt).minusMinutes(target.lookbackMinutes)
            adjusted.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                .also { log.info("[{}] Lookback {}min: wm adjusted {} -> {}", t, target.lookbackMinutes, rawWm, it) }
        } else {
            rawWm
        }

        // 4. Build the paginated query using tuple comparison on (wm, key1, key2, ...).
        //    Rationale: the naive `wm > ?` boundary loses rows that share the same watermark
        //    as the last row of the previous page. Tuple pagination — ORDER BY (wm, keys...)
        //    + WHERE (wm > ? OR (wm = ? AND (keys) > (?,?,...))) — is a strict-increase
        //    seek that works correctly even for composite keys and watermark ties.
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

        // 5. Stream source → staging without an intermediate buffer.
        //    Each source row is pushed directly into the staging PreparedStatement's batch;
        //    executeBatch fires every BATCH_SIZE rows. Memory footprint is O(BATCH_SIZE),
        //    not O(rows). setFetchSize tells the JDBC driver to prefetch batches worth of
        //    rows per round-trip so we aren't blocked on network for every row.
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
                // Use ResultSet metadata rather than a config column list — so the only
                // contract with targets.json is that SELECT's column order matches the
                // INSERT's (?,?,...) placeholders.
                val colCount = rs.metaData.columnCount
                val keyIndices = target.keyColumns.map { rs.findColumn(it) }
                oracleConn.prepareStatement(target.targetInsert).use { insPs ->
                    while (rs.next()) {
                        for (i in 1..colCount) {
                            insPs.setObject(i, rs.getObject(i))
                        }
                        // Record key values of THIS row — after the loop ends, this holds
                        // the last row's keys, which seed tuple pagination for the next page.
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

        // 6. Snapshot the new high-water mark from the staging we just wrote.
        //    Using staging (not source) lets us be sure the watermark we persist matches
        //    what actually landed — any row filtered out between source and staging won't
        //    advance the mark past data we haven't stored.
        val newWm = oracleConn.createStatement().use { st ->
            st.executeQuery("SELECT MAX(${target.watermarkColumn}) FROM ${target.stagingTable}").use { wmRs ->
                if (wmRs.next()) wmRs.getString(1) else null
            }
        }
        return LoadResult(count, newWm ?: lastWm, newLastKeys)
    }
}
