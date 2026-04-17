package app.db

import oracle.ucp.jdbc.PoolDataSource
import oracle.ucp.jdbc.PoolDataSourceFactory
import org.slf4j.LoggerFactory
import java.sql.Connection

/**
 * Singleton connection pool (UCP) for Oracle Autonomous Database — the ETL target.
 *
 * Uses Oracle's Universal Connection Pool with wallet-based mTLS when TNS_ADMIN is
 * set (required for ATP). The pool is created lazily on first use so the app can
 * start even when ATP is temporarily unreachable; connection failure surfaces only
 * when an ETL run or health check actually needs a connection.
 *
 * Configuration is entirely env-var driven (DB_USER, DB_PASSWORD, DB_CONNECT_STRING,
 * optional TNS_ADMIN) — no secrets live in the codebase.
 */
object OracleDs {
    private val log = LoggerFactory.getLogger(OracleDs::class.java)

    /**
     * Lazy UCP pool. Pool sizing (initial=2, min=2, max=10) is conservative on purpose:
     * a handful of concurrent ETL threads plus the health check is the realistic load,
     * and ATP connection-count quotas favour small pools.
     */
    private val pool: PoolDataSource by lazy {
        val walletPath = System.getenv("TNS_ADMIN")
        if (walletPath != null) {
            System.setProperty("oracle.net.tns_admin", walletPath)
        }
        PoolDataSourceFactory.getPoolDataSource().apply {
            connectionFactoryClassName = "oracle.jdbc.pool.OracleDataSource"
            val connectString = System.getenv("DB_CONNECT_STRING") ?: error("DB_CONNECT_STRING not set")
            url = if (walletPath != null)
                "jdbc:oracle:thin:@${connectString}?TNS_ADMIN=${walletPath}"
            else
                "jdbc:oracle:thin:@${connectString}"
            user = System.getenv("DB_USER") ?: error("DB_USER not set")
            password = System.getenv("DB_PASSWORD") ?: error("DB_PASSWORD not set")
            connectionPoolName = "JDBC_UCP_POOL"
            initialPoolSize = 2
            minPoolSize = 2
            maxPoolSize = 10
        }.also { log.info("UCP pool created, url={}", it.url) }
    }

    /**
     * Borrow a connection from the pool. Caller owns closing it (use try-with-resources /
     * Kotlin `use {}`) — closing returns the connection to the pool, it is not destroyed.
     */
    fun getConnection(): Connection = pool.connection
}
