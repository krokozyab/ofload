package app.db

import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.locks.ReentrantLock

/**
 * Single process-wide source connection to Oracle Fusion via the ofjdbc driver.
 *
 * Why one connection, not a pool or per-worker:
 *   ofjdbc opens an interactive SSO (Oracle IDCS) flow in the browser on every
 *   `DriverManager.getConnection()` call. Having N per-target connections means N
 *   browser windows pop open, which is unacceptable for humans running the service.
 *   Instead we hold exactly one connection process-wide and serialise access to it
 *   via [mutex]. Parallel ETL workers take turns reading from source; the SSO flow
 *   happens once per service lifetime (or once per re-authentication after the
 *   remote session expires).
 *
 * Thread safety: every caller must go through [withConnection], which acquires the
 * mutex before handing over the shared [Connection]. Never stash the returned
 * Connection reference outside the lambda — that would re-introduce the race
 * condition we are solving here.
 *
 * Auto-reconnect: if the block throws what looks like an auth/session failure the
 * connection is discarded and the next [withConnection] call re-establishes it
 * (that re-invocation is what triggers a fresh SSO). One reconnect = one new SSO,
 * still amortised across all subsequent callers.
 */
object SourceDs : AutoCloseable {
    private val log = LoggerFactory.getLogger(SourceDs::class.java)

    @Volatile private var conn: Connection? = null
    private val mutex = ReentrantLock()

    init {
        val driverClass = System.getenv("SOURCE_DRIVER_CLASS") ?: error("SOURCE_DRIVER_CLASS not set")
        Class.forName(driverClass)
    }

    /**
     * Execute [block] with exclusive access to the shared source connection.
     *
     * Opens the connection lazily on first call (triggers SSO); subsequent calls
     * reuse the same connection. If [block] throws what looks like an auth failure
     * the connection is closed and nullified so the *next* call opens a fresh one.
     * Any other exception is propagated unchanged and the connection stays open.
     *
     * Holds the mutex for the entire duration of [block]. For streaming loads this
     * can be minutes — other callers wait. That is an acceptable cost of keeping
     * SSO to a single browser window.
     */
    fun <T> withConnection(block: (Connection) -> T): T {
        mutex.lock()
        try {
            val c = conn ?: openConnection().also { conn = it }
            return try {
                block(c)
            } catch (e: Exception) {
                if (isAuthError(e)) {
                    log.warn("Source auth error — discarding connection, next call will re-authenticate")
                    runCatching { c.close() }
                    conn = null
                }
                throw e
            }
        } finally {
            mutex.unlock()
        }
    }

    /**
     * Lazily establish the source connection. First call in a process triggers the SSO handshake.
     *
     * Prefetch of next page is disabled (`ofjdbc.prefetch.enabled=false`) because this
     * ETL enforces a per-page row cap on the Java side and closes the ResultSet once
     * `pageSize` rows are consumed. The driver's default 75%-prefetch would fire a
     * redundant WSDL round-trip that gets cancelled by `rs.close()` — pure network
     * overhead. For "read-to-end" consumers (BI reports, full dumps) the default is
     * still the right choice; this flag is per-connection.
     */
    private fun openConnection(): Connection {
        log.info("Opening source connection (interactive SSO on first open)...")
        val url = System.getenv("SOURCE_URL") ?: error("SOURCE_URL not set")
        val props = Properties().apply {
            System.getenv("SOURCE_USER")?.let { setProperty("user", it) }
            System.getenv("SOURCE_PASSWORD")?.let { setProperty("password", it) }
            setProperty("ofjdbc.prefetch.enabled", "false")
        }
        return DriverManager.getConnection(url, props)
            .also { log.info("Source connected (prefetch disabled)") }
    }

    /**
     * Heuristic classifier for "is this exception worth discarding the connection for?".
     * Matches on exception message text — ofjdbc surfaces auth failures as generic
     * SQLExceptions without a dedicated vendor code, so substring matching is the
     * only signal we have. False positives only cost one extra reconnect.
     */
    private fun isAuthError(e: Throwable): Boolean {
        val msg = e.message?.lowercase() ?: ""
        return msg.contains("401") ||
               msg.contains("unauthorized") ||
               msg.contains("authentication") ||
               msg.contains("token") ||
               msg.contains("session") ||
               msg.contains("login")
    }

    /**
     * Close the shared connection. Called from Main's shutdown hook so the remote
     * session is torn down cleanly on SIGTERM; the next process invocation will
     * open a fresh connection and trigger a new SSO.
     */
    override fun close() {
        mutex.lock()
        try {
            conn?.let {
                log.info("Closing source connection")
                runCatching { it.close() }
            }
            conn = null
        } finally {
            mutex.unlock()
        }
    }
}
