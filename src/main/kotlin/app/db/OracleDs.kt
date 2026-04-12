package app.db

import oracle.ucp.jdbc.PoolDataSource
import oracle.ucp.jdbc.PoolDataSourceFactory
import org.slf4j.LoggerFactory
import java.sql.Connection

object OracleDs {
    private val log = LoggerFactory.getLogger(OracleDs::class.java)
    private val pool: PoolDataSource

    init {
        val walletPath = System.getenv("TNS_ADMIN")
        if (walletPath != null) {
            System.setProperty("oracle.net.tns_admin", walletPath)
        }
        pool = PoolDataSourceFactory.getPoolDataSource().apply {
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
        }
        log.info("UCP pool created, url={}", pool.url)
    }

    fun getConnection(): Connection = pool.connection
}
