package app

import oracle.ucp.jdbc.PoolDataSource
import oracle.ucp.jdbc.PoolDataSourceFactory
import java.sql.SQLException

class UCPDataSource {
    private val poolDataSource: PoolDataSource

    init {
        val walletPath = System.getenv("TNS_ADMIN")
        if (walletPath != null) {
            System.setProperty("oracle.net.tns_admin", walletPath)
        }

        this.poolDataSource = PoolDataSourceFactory.getPoolDataSource()
        poolDataSource.setConnectionFactoryClassName(CONN_FACTORY_CLASS_NAME)

        val url = if (walletPath != null) {
            // Wallet mode: use TNS alias from tnsnames.ora (e.g. "ubeb4ocafu6uaz8o_medium")
            "jdbc:oracle:thin:@${CONNECT_STRING}?TNS_ADMIN=${walletPath}"
        } else {
            "jdbc:oracle:thin:@${CONNECT_STRING}"
        }
        poolDataSource.setURL(url)
        poolDataSource.setUser(DB_USER)
        poolDataSource.setPassword(DB_PASSWORD)
        poolDataSource.setConnectionPoolName("JDBC_UCP_POOL")
    }

    fun testConnection() {
        try {
            poolDataSource.getConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT * FROM test_table").use { rs ->
                        while (rs.next()) {
                            println("Oracle Connection is working! Query result: " + rs.getString(2))
                        }
                    }
                }
            }
        } catch (e: SQLException) {
            var cause: Throwable? = e
            while (cause != null) {
                println("Caused by: ${cause::class.simpleName}: ${cause.message}")
                cause = cause.cause
            }
        }
    }

    companion object {
        private val DB_USER = System.getenv("DB_USER") ?: error("DB_USER env var not set")
        private val DB_PASSWORD = System.getenv("DB_PASSWORD") ?: error("DB_PASSWORD env var not set")
        private val CONNECT_STRING = System.getenv("DB_CONNECT_STRING") ?: error("DB_CONNECT_STRING env var not set")
        private const val CONN_FACTORY_CLASS_NAME = "oracle.jdbc.replay.OracleConnectionPoolDataSourceImpl"

        @JvmStatic
        fun main(args: Array<String>) {
            try {
                val uds = UCPDataSource()
                uds.testConnection()
                println("Ok")
            } catch (e: SQLException) {
                e.printStackTrace()
            }
        }
    }
}
