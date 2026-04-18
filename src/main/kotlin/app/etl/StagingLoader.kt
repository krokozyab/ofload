package app.etl

import app.config.TargetConfig
import app.metrics.Metrics
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
     * Required placeholder in [app.config.TargetConfig.sourceQuery] that marks where
     * the watermark predicate gets injected on each page. Replaced with `wmCol > ?`
     * on the first page and a tuple-seek expression on subsequent pages.
     */
    internal const val WM_MARKER = "/*WM*/"

    /**
     * Upper bound on how long one page fetch (one entire ResultSet consumption across
     * the driver's internal OFFSET/FETCH NEXT pagination AND its retry loop) is
     * allowed to take before the source call is aborted.
     *
     * Honoured by ofjdbc's [java.sql.Statement.setQueryTimeout]: the driver records
     * a deadline at `executeQuery()` and re-checks it before every HTTP round-trip
     * plus every retry backoff (`PaginatedResultSet.fetchPageXml`). Exceeding it
     * raises `SQLTimeoutException`, which propagates to [Pipeline.runTarget], marks
     * the run FAILED, and releases the [app.db.SourceDs] singleton mutex so other
     * targets can proceed.
     *
     * The default (600 s = 10 min) is deliberately above ofjdbc's per-request read
     * timeout (120 s) × retry attempts (3) + exponential backoff (≤30 s), so the
     * driver's own timeouts fire first on individual round-trips and retries work
     * normally. Our cap only triggers if the overall page fetch becomes pathological.
     */
    private val SOURCE_QUERY_TIMEOUT_SECONDS: Int =
        System.getenv("SOURCE_QUERY_TIMEOUT_SECONDS")?.toIntOrNull() ?: 600

    /**
     * Build the paginated SELECT that will be sent to the source driver.
     *
     * Pure function, no DB access — lives outside [load] so unit tests can exercise
     * every branch without a source connection. Injects the appropriate watermark
     * predicate into [app.config.TargetConfig.sourceQuery] at [WM_MARKER] and appends
     * `ORDER BY` when the target is paginated.
     *
     * @param lastKeyValues null → first-page (scalar `wm > ?`); non-null → seek form.
     */
    internal fun buildSourceQuery(target: TargetConfig, lastKeyValues: List<String?>?): String {
        require(target.sourceQuery.contains(WM_MARKER)) {
            "targets.json: sourceQuery for '${target.name}' must contain $WM_MARKER where the watermark predicate should be injected"
        }
        val keyColsCsv = target.keyColumns.joinToString(", ")
        val keyPlaceholders = target.keyColumns.joinToString(", ") { "?" }
        val firstPagePredicate = "${target.watermarkColumn} > ?"
        // Scalar form for single-column keys — the row-value form `(col) > (?)` is
        // semantically equivalent in Oracle but observed to confuse the BI Publisher
        // SQL parser through ofjdbc's RP_ARB.xdo transport. Use scalar when we can;
        // tuple is reserved for genuinely composite keys where it's required.
        val seekPredicate = if (target.keyColumns.size == 1) {
            val k = target.keyColumns[0]
            "(${target.watermarkColumn} > ? OR (${target.watermarkColumn} = ? AND $k > ?))"
        } else {
            "(${target.watermarkColumn} > ? OR (${target.watermarkColumn} = ? AND ($keyColsCsv) > ($keyPlaceholders)))"
        }
        return if (target.pageSize != null) {
            val predicate = if (lastKeyValues != null) seekPredicate else firstPagePredicate
            val baseQuery = target.sourceQuery.replace(WM_MARKER, predicate)
            "$baseQuery ORDER BY ${target.watermarkColumn}, $keyColsCsv"
        } else {
            target.sourceQuery.replace(WM_MARKER, firstPagePredicate)
        }
    }

    /**
     * Compute the effective watermark to bind into the source query for the current page.
     *
     * Pure function. On the first page of a run with [TargetConfig.lookbackMinutes] set,
     * subtract that many minutes from the persisted watermark to re-scan recently-updated
     * rows (guards against source-side clock skew and late-arriving updates). Second+
     * pages within the same run use the raw watermark — we've already scanned the
     * lookback window. First-ever run (lastWm=null) falls back to [TargetConfig.initialWm].
     */
    internal fun computeEffectiveWm(target: TargetConfig, lastWm: String?, lastKeyValues: List<String?>?): String {
        val rawWm = lastWm ?: target.initialWm
        return if (target.lookbackMinutes != null && lastKeyValues == null && lastWm != null) {
            val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]")
            val adjusted = LocalDateTime.parse(rawWm, fmt).minusMinutes(target.lookbackMinutes)
            adjusted.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        } else {
            rawWm
        }
    }

    /**
     * Assemble the ordered list of bind values for the query returned by [buildSourceQuery].
     *
     * First page: `[wm]` — one bind for the scalar `wm > ?` predicate.
     * Seek page: `[wm, wm, key1, key2, ...]` — two `?`s for the watermark comparison and
     * one `?` per key column in the tuple comparison.
     */
    internal fun buildBindParams(target: TargetConfig, wm: String, lastKeyValues: List<String?>?): List<String?> {
        val params = mutableListOf<String?>(wm)
        if (target.pageSize != null && lastKeyValues != null) {
            params.add(wm)
            params.addAll(lastKeyValues)
        }
        return params
    }

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

        // 3. Resolve the effective watermark for this page (handles lookback on 1st page).
        val wm = computeEffectiveWm(target, lastWm, lastKeyValues)
        if (target.lookbackMinutes != null && lastKeyValues == null && lastWm != null && wm != lastWm) {
            log.info("[{}] Lookback {}min: wm adjusted {} -> {}", t, target.lookbackMinutes, lastWm, wm)
        }

        // 4. Build the paginated query. Pagination is delegated to the ofjdbc driver's
        //    PaginatedResultSet, which appends `OFFSET x ROWS FETCH NEXT y ROWS ONLY`
        //    to any SELECT that doesn't already contain ROWNUM/OFFSET/FETCH. See
        //    PaginatedResultSet.rewriteQueryForPagination in the ofjdbc source.
        val query = buildSourceQuery(target, lastKeyValues)

        // 5. Stream source → staging without an intermediate buffer.
        //    Each source row is pushed directly into the staging PreparedStatement's batch;
        //    executeBatch fires every BATCH_SIZE rows. Memory footprint is O(BATCH_SIZE),
        //    not O(rows). setFetchSize tells the JDBC driver to prefetch batches worth of
        //    rows per round-trip so we aren't blocked on network for every row.
        log.info("[{}] Streaming from source (wm={}, lastKeys={}, pageSize={})",
            t, wm, lastKeyValues, target.pageSize ?: "unlimited")

        // Page-size is enforced on the Java side as a second line of defense: the outer
        // `SELECT * FROM (...) WHERE ROWNUM <= N` wrapper is not always respected by the
        // source (e.g. the BI Publisher universal-SQL endpoint RP_ARB.xdo observably
        // returns all matching rows regardless of ROWNUM). srcPs.maxRows is the JDBC
        // contract hint; the while-loop check below is the guarantee.
        val pageLimit = target.pageSize?.toLong() ?: Long.MAX_VALUE

        val startStream = System.currentTimeMillis()
        var count = 0L
        var newLastKeys: List<String?>? = null

        sourceConn.prepareStatement(query).use { srcPs ->
            // fetchSize drives ofjdbc's internal OFFSET/FETCH NEXT rewrite — each
            // WSDL round-trip pulls this many rows. Our page size (target.pageSize)
            // is enforced on the Java side in the while-loop below.
            srcPs.fetchSize = target.pageSize ?: BATCH_SIZE
            // srcPs.maxRows is intentionally NOT set — ofjdbc does not honour the
            // JDBC-level row limit. Row cap is enforced by the Java-side break
            // below; the driver's auto-pagination lets us close the ResultSet
            // cleanly when we've consumed enough.
            // Overall ceiling on one page fetch — the driver's own read-timeout +
            // retry logic sit underneath this and will handle transient failures;
            // this only fires if the whole thing is genuinely pathological.
            srcPs.queryTimeout = SOURCE_QUERY_TIMEOUT_SECONDS
            buildBindParams(target, wm, lastKeyValues).forEachIndexed { i, v ->
                srcPs.setString(i + 1, v)
            }
            srcPs.executeQuery().use { rs ->
                // Use ResultSet metadata rather than a config column list — so the only
                // contract with targets.json is that SELECT's column order matches the
                // INSERT's (?,?,...) placeholders.
                val colCount = rs.metaData.columnCount
                val keyIndices = target.keyColumns.map { rs.findColumn(it) }
                oracleConn.prepareStatement(target.targetInsert).use { insPs ->
                    while (count < pageLimit && rs.next()) {
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
        if (target.pageSize != null && count >= target.pageSize) {
            log.info("[{}] Page filled to pageSize={}; RS closed, next page will resume via seek predicate", t, target.pageSize)
        }
        log.info("[{}] Source: {} rows read+staged in {}ms", t, count, streamTime)

        // Metrics: record page duration + rows staged. Zero-row pages still count —
        // their duration is informative signal for "how fast does the source confirm
        // there are no new rows".
        Metrics.pageTimer(t).record(java.time.Duration.ofMillis(streamTime))
        if (count > 0) Metrics.rowsLoaded(t, count)

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
