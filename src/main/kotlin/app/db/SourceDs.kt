package app.db

import java.sql.Connection
import java.sql.DriverManager

/**
 * Factory for Oracle Fusion source connections via the ofjdbc driver.
 *
 * Unlike the ATP side there is NO pool here — each call to [getConnection] opens
 * a fresh JDBC connection. That is deliberate for now: ofjdbc wraps a BI Publisher
 * REST endpoint, and pooling a driver with its own session/auth lifecycle added
 * more problems than it solved. Pooling remains on the backlog to be revisited if
 * connection-open latency becomes a bottleneck.
 *
 * The driver class name is loaded once in [init] from SOURCE_DRIVER_CLASS so the
 * JDBC DriverManager registers it before any getConnection() call.
 */
object SourceDs {
    init {
        val driverClass = System.getenv("SOURCE_DRIVER_CLASS") ?: error("SOURCE_DRIVER_CLASS not set")
        Class.forName(driverClass)
    }

    /**
     * Open a new source connection. Caller must close it (typically via Kotlin `use {}`).
     * SOURCE_URL/SOURCE_USER/SOURCE_PASSWORD env vars drive authentication — secrets never
     * land in code or config files.
     */
    fun getConnection(): Connection {
        val url = System.getenv("SOURCE_URL") ?: error("SOURCE_URL not set")
        return DriverManager.getConnection(url, System.getenv("SOURCE_USER"), System.getenv("SOURCE_PASSWORD"))
    }
}
