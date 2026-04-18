package app.db

import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.locks.ReentrantLock

/**
 * Per-call source connection factory for Oracle Fusion via the ofjdbc driver.
 *
 * **Parallelism model:**
 * Each [withConnection] call receives its own dedicated [Connection] — no shared
 * state between callers, no serialisation on use. Multiple ETL workers read from
 * source concurrently, limited only by `ETL_WORKERS`.
 *
 * **SSO dedup (why parallel reads don't open multiple browsers):**
 * ofjdbc owns a process-wide `my.jdbc.wsdl_driver.auth.TokenCache` (a
 * ConcurrentHashMap<host, Token> with ~55 min TTL). `BrowserAuthenticator.authenticate()`
 * checks this cache before launching a browser. In steady state every connection
 * creation after the first reuses the cached JWT silently.
 *
 * **The race we guard against:**
 * The driver's cache-check → browser-launch → cache-put is NOT guarded by a lock
 * in the driver itself. Three workers starting simultaneously all miss the cache
 * and all open a browser. [openLock] around our [DriverManager.getConnection] call
 * serialises exactly that critical section — the first caller misses the cache
 * and triggers SSO, the second and third enter the lock, see the now-cached
 * token, return immediately. Net: one browser per token lifetime, and parallel
 * source access the rest of the time.
 *
 * The lock is held only for connection creation, which after the first call is
 * just "make WsdlConnection object, set auth header from cache" — microseconds.
 * Actual query execution happens on the returned Connection, outside the lock,
 * fully parallel across callers.
 */
object SourceDs : AutoCloseable {
    private val log = LoggerFactory.getLogger(SourceDs::class.java)
    private val openLock = ReentrantLock()

    init {
        val driverClass = System.getenv("SOURCE_DRIVER_CLASS") ?: error("SOURCE_DRIVER_CLASS not set")
        Class.forName(driverClass)
    }

    /**
     * Execute [block] with a freshly opened source [Connection]. The connection is
     * closed automatically when the block returns (or throws). Each invocation is
     * independent — concurrent calls from different threads run in parallel.
     *
     * First caller per process (or per token expiry) pays the SSO-browser cost
     * while under [openLock]; everyone after sees a cached token and creation is
     * near-instant.
     */
    fun <T> withConnection(block: (Connection) -> T): T {
        val conn = openSerialisedForAuth()
        try {
            return block(conn)
        } finally {
            runCatching { conn.close() }
        }
    }

    /**
     * Create a new connection with the auth-critical section serialised. The lock
     * is held for the full [DriverManager.getConnection] call so two cold-cache
     * callers can't both launch browsers. Subsequent callers find a warm cache and
     * fly through the same critical section in microseconds.
     */
    private fun openSerialisedForAuth(): Connection {
        val url = System.getenv("SOURCE_URL") ?: error("SOURCE_URL not set")
        val props = Properties().apply {
            // Auth type selection — driver expects explicit `authType` property.
            // When SOURCE_AUTH_TYPE is unset, the driver falls back to its legacy
            // BASIC-by-default branch which requires both user and password. Setting
            // SOURCE_AUTH_TYPE=BROWSER enables SSO (opens a browser on first use,
            // cached token thereafter). BEARER reads the raw JWT from SOURCE_PASSWORD.
            System.getenv("SOURCE_AUTH_TYPE")?.let { setProperty("authType", it) }
            System.getenv("SOURCE_USER")?.let { setProperty("user", it) }
            System.getenv("SOURCE_PASSWORD")?.let { setProperty("password", it) }
            System.getenv("SOURCE_SSO_TIMEOUT")?.let { setProperty("ssoTimeout", it) }
            // ETL uses a Java-side pageSize cap + rs.close() — the driver's 75%-
            // prefetch would fire a redundant WSDL round-trip every page that then
            // gets cancelled. Pure network overhead in our usage; leave it off.
            setProperty("ofjdbc.prefetch.enabled", "false")
        }
        openLock.lock()
        try {
            return DriverManager.getConnection(url, props).also {
                log.debug("Source connection opened (token cache hit or fresh SSO)")
            }
        } finally {
            openLock.unlock()
        }
    }

    /**
     * No-op for backward compatibility with the old singleton-connection design.
     * There is no shared connection to tear down any more; each one is closed by
     * the `use {}` inside [withConnection]. The driver's `TokenCache` is its own
     * singleton and handles its own shutdown.
     */
    override fun close() {
        // nothing to do
    }
}
