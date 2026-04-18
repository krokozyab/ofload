package app.server

import app.db.OracleDs
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Cached `SELECT COUNT(*) FROM <targetTable>` values with async refresh.
 *
 * **Why:** the dashboard polls `/status` every 15 s and each poll used to fire one
 * COUNT(*) per target. On small tables (54 k rows) it's ~100 ms per query — fine.
 * On multi-million-row targets COUNT can take 1–5 s, and three of them serialised
 * inside one HTTP handler makes the dashboard visibly sluggish. The fix is to cache
 * the value with a TTL and refresh it on a background thread so `/status` never
 * blocks on a slow query.
 *
 * **Semantics:**
 *   * [get] returns the cached value immediately (never blocks on DB), even if the
 *     cache entry is stale or missing.
 *   * If the cached entry is stale (older than [ttlSeconds]) or missing, a single
 *     async refresh is kicked off on the background executor. Subsequent calls for
 *     the same target while the refresh is in flight don't spawn duplicates.
 *   * First call for a target returns `null` (no cached value yet). The UI renders
 *     this as `-` until the background refresh completes on the next poll.
 *   * Failed refreshes keep the previous cached value intact and are retried on
 *     the next stale read.
 *
 * **Threading:** all state lives in [ConcurrentHashMap]s; the refresh executor is a
 * single daemon thread so background load on the UCP pool stays bounded (one COUNT
 * in flight at a time across all targets).
 */
class TargetRowCountCache(
    private val ttlSeconds: Long = (System.getenv("TARGET_ROW_COUNT_TTL_SECONDS")?.toLongOrNull() ?: 60)
) : AutoCloseable {

    private val log = LoggerFactory.getLogger(TargetRowCountCache::class.java)

    private data class Entry(val value: Long?, val refreshedAtMs: Long)

    private val cache = ConcurrentHashMap<String, Entry>()
    private val inFlight = ConcurrentHashMap<String, Boolean>()

    private val executor = Executors.newSingleThreadExecutor { r ->
        Thread(r, "row-count-cache").apply { isDaemon = true }
    }

    /**
     * Return the cached count for [targetName] (or null when nothing has been cached
     * yet). Triggers an async refresh if the cache is stale or empty. Never blocks
     * on the DB — even a brand-new call returns immediately with null.
     */
    fun get(targetName: String, tableName: String): Long? {
        val entry = cache[targetName]
        val now = System.currentTimeMillis()
        val isStale = entry == null || (now - entry.refreshedAtMs) >= ttlSeconds * 1000L
        if (isStale) triggerRefresh(targetName, tableName)
        return entry?.value
    }

    /**
     * Submit a background refresh for this target, deduplicated so only one refresh
     * is in flight per target at a time. Failures are logged and leave the prior
     * cached value untouched.
     */
    private fun triggerRefresh(targetName: String, tableName: String) {
        if (inFlight.putIfAbsent(targetName, true) != null) return
        executor.submit {
            try {
                val count = queryCount(tableName)
                cache[targetName] = Entry(count, System.currentTimeMillis())
            } catch (e: Exception) {
                log.debug("Row count refresh failed for {}: {}", tableName, e.message)
                // Leave previous entry in place; retry on next stale read.
            } finally {
                inFlight.remove(targetName)
            }
        }
    }

    /**
     * Actually run the COUNT against the target table. Uses its own pool connection
     * so it doesn't share state with the /status request handler. Returns null when
     * the table does not exist yet (e.g. before initial DDL) or on any SQL error.
     */
    private fun queryCount(tableName: String): Long? =
        OracleDs.getConnection().use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(*) FROM $tableName").use { rs ->
                    if (rs.next()) rs.getLong(1) else null
                }
            }
        }

    /**
     * Shutdown the background executor, called from RunManager.shutdown to honour
     * the service's graceful-stop contract. Waits briefly so an in-flight COUNT
     * has a chance to finish cleanly before the JVM exits.
     */
    override fun close() {
        executor.shutdown()
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            executor.shutdownNow()
        }
    }
}
