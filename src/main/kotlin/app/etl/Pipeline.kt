package app.etl

import app.config.PipelineConfig
import app.config.TargetConfig
import app.db.OracleDs
import app.db.SourceDs
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.sql.Connection
import java.util.concurrent.Executors

class Pipeline(private val cfg: PipelineConfig) {
    private val log = LoggerFactory.getLogger(Pipeline::class.java)

    companion object {
        private const val MAX_SOURCE_RETRIES = 2
    }

    /**
     * Holds a shared source connection that can be reconnected on auth failure.
     * Thread-safe: reconnect is synchronized so only one thread triggers SSO.
     */
    class SourceConnHolder(initial: Connection) : AutoCloseable {
        @Volatile var conn: Connection = initial
            private set

        @Synchronized
        fun reconnect(): Connection {
            val log = LoggerFactory.getLogger(SourceConnHolder::class.java)
            log.warn("Reconnecting to source (SSO refresh)...")
            runCatching { conn.close() }
            conn = SourceDs.getConnection()
            log.info("Source reconnected")
            return conn
        }

        override fun close() {
            runCatching { conn.close() }
        }
    }

    fun run() = runTargets(cfg.targets)

    fun runTargets(targets: List<TargetConfig>) {
        var seqId = Int.MIN_VALUE
        val groups = targets
            .map { it to (it.group ?: seqId--) }
            .groupBy({ it.second }, { it.first })
            .toSortedMap()

        for ((groupId, grpTargets) in groups) {
            if (grpTargets.size == 1) {
                runTarget(grpTargets[0])
            } else {
                MDC.put("group", groupId.toString())
                log.info("Group {}: running {} in parallel", groupId, grpTargets.map { it.name })
                log.info("Group {}: connecting to source (single SSO)", groupId)
                SourceConnHolder(SourceDs.getConnection()).use { holder ->
                    log.info("Group {}: source connected", groupId)
                    val pool = Executors.newFixedThreadPool(grpTargets.size)
                    val futures = grpTargets.map { target ->
                        pool.submit<Unit> {
                            MDC.put("group", groupId.toString())
                            runTarget(target, holder)
                            MDC.remove("group")
                        }
                    }
                    futures.forEach { it.get() }
                    pool.shutdown()
                }
                log.info("Group {}: done", groupId)
                MDC.remove("group")
            }
        }
    }

    private fun runTarget(target: TargetConfig, sourceHolder: SourceConnHolder? = null) {
        val t = target.name
        MDC.put("target", t)
        log.info("[{}] Starting", t)
        OracleDs.getConnection().use { oracleConn ->
            log.info("[{}] ATP connected", t)
            oracleConn.autoCommit = false
            try {
                var lastWm = WatermarkStore.read(oracleConn, t)
                WatermarkStore.beginRun(oracleConn, t, target.watermarkColumn)
                oracleConn.commit()

                var totalLoaded = 0L
                var totalMerged = 0L
                var pageNum = 0
                var lastKeyValue: String? = null

                do {
                    pageNum++
                    MDC.put("page", pageNum.toString())

                    val result = loadPageWithRetry(target, sourceHolder, oracleConn, lastWm, lastKeyValue, pageNum)

                    if (result.rowsLoaded == 0L) break

                    val startMerge = System.currentTimeMillis()
                    val merged = TargetMerger.merge(oracleConn, target)
                    log.info("[{}] Merged {} rows in {}ms", t, merged, System.currentTimeMillis() - startMerge)
                    totalLoaded += result.rowsLoaded
                    totalMerged += merged
                    lastWm = result.newWm
                    lastKeyValue = result.lastKeyValue

                    WatermarkStore.finishRun(oracleConn, t, lastWm, totalLoaded)
                    oracleConn.commit()

                    if (target.pageSize != null) {
                        log.info("[{}] Page {}: loaded={}, merged={} | Total: loaded={}, merged={} | wm={}",
                            t, pageNum, result.rowsLoaded, merged, totalLoaded, totalMerged, lastWm)
                    }
                } while (target.pageSize != null && result.rowsLoaded.toInt() == target.pageSize)

                WatermarkStore.finishRun(oracleConn, t, lastWm, totalLoaded)
                oracleConn.commit()
                log.info("[{}] OK: total_loaded={}, total_merged={}, new_wm={}", t, totalLoaded, totalMerged, lastWm)
            } catch (e: Throwable) {
                runCatching { oracleConn.rollback() }
                runCatching {
                    OracleDs.getConnection().use { failConn ->
                        failConn.autoCommit = true
                        WatermarkStore.failRun(failConn, t, e.message ?: "unknown error")
                    }
                }.onFailure { log.warn("[{}] Failed to record FAILED status: {}", t, it.message) }
                log.error("[{}] FAILED: {}", t, e.message, e)
                if (e is Error) throw e
            }
        }
        MDC.remove("target")
        MDC.remove("page")
    }

    private fun loadPageWithRetry(
        target: TargetConfig,
        sourceHolder: SourceConnHolder?,
        oracleConn: Connection,
        lastWm: String?,
        lastKeyValue: String?,
        pageNum: Int
    ): LoadResult {
        val t = target.name
        var lastError: Exception? = null

        for (attempt in 1..MAX_SOURCE_RETRIES + 1) {
            try {
                return if (sourceHolder != null) {
                    log.info("[{}] Loading page {} (wm={}, lastKey={})", t, pageNum, lastWm, lastKeyValue)
                    StagingLoader.load(target, sourceHolder.conn, oracleConn, lastWm, lastKeyValue)
                } else {
                    log.info("[{}] Connecting to source", t)
                    SourceDs.getConnection().use { srcConn ->
                        log.info("[{}] Source connected, loading page {} (wm={}, lastKey={})", t, pageNum, lastWm, lastKeyValue)
                        StagingLoader.load(target, srcConn, oracleConn, lastWm, lastKeyValue)
                    }
                }
            } catch (e: Exception) {
                lastError = e
                if (attempt <= MAX_SOURCE_RETRIES && isSourceAuthError(e)) {
                    log.warn("[{}] Source error on attempt {}/{}: {}. Reconnecting...",
                        t, attempt, MAX_SOURCE_RETRIES + 1, e.message)
                    if (sourceHolder != null) {
                        sourceHolder.reconnect()
                    }
                    // Staging was truncated, safe to retry
                } else {
                    throw e
                }
            }
        }
        throw lastError!!
    }

    private fun isSourceAuthError(e: Exception): Boolean {
        val msg = e.message?.lowercase() ?: ""
        return msg.contains("401") ||
               msg.contains("unauthorized") ||
               msg.contains("authentication") ||
               msg.contains("token") ||
               msg.contains("session") ||
               msg.contains("login")
    }
}
