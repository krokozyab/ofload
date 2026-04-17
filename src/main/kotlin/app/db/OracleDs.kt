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
 *
 * Session state policy: every borrow re-applies a known NLS baseline via ALTER SESSION
 * (see [applyBaseline]). ETL code is free to add target-specific overrides on top
 * during a run; those overrides are wiped on the next borrow so state never leaks
 * between targets via the pool. UCP's ConnectionInitializationCallback is not used
 * because without connection labelling it only fires on physical connect, not on
 * logical borrow — the [getConnection] hook is a reliable equivalent.
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
     * Borrow a connection from the pool and reset its session to a known NLS baseline.
     * Caller owns closing it (use try-with-resources / Kotlin `use {}`) — closing returns
     * the connection to the pool, it is not destroyed.
     */
    fun getConnection(): Connection {
        val conn = pool.connection
        applyBaseline(conn)
        return conn
    }

    /**
     * Reset the pooled session to a deterministic NLS baseline so target N never sees
     * NLS settings left behind by target N-1. Combined into a single ALTER SESSION so
     * the reset costs one round-trip, not three.
     */
    private fun applyBaseline(conn: Connection) {
        conn.createStatement().use { st ->
            st.execute(
                "ALTER SESSION SET " +
                    "NLS_DATE_FORMAT='YYYY-MM-DD' " +
                    "NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF' " +
                    "NLS_NUMERIC_CHARACTERS='.,'"
            )
        }
    }
}
