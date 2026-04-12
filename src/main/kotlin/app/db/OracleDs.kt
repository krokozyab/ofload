package app.db

import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties

object OracleDs {
    private val url: String
    private val props: Properties

    init {
        val walletPath = System.getenv("TNS_ADMIN")
        if (walletPath != null) {
            System.setProperty("oracle.net.tns_admin", walletPath)
        }
        val connectString = System.getenv("DB_CONNECT_STRING") ?: error("DB_CONNECT_STRING not set")
        url = if (walletPath != null)
            "jdbc:oracle:thin:@${connectString}?TNS_ADMIN=${walletPath}"
        else
            "jdbc:oracle:thin:@${connectString}"

        props = Properties().apply {
            setProperty("user", System.getenv("DB_USER") ?: error("DB_USER not set"))
            setProperty("password", System.getenv("DB_PASSWORD") ?: error("DB_PASSWORD not set"))
            setProperty("oracle.jdbc.loginTimeout", "30")
        }

        Class.forName("oracle.jdbc.OracleDriver")
        println("[OracleDs] URL: $url")
    }

    fun getConnection(): Connection {
        DriverManager.setLoginTimeout(30)
        return DriverManager.getConnection(url, props)
    }
}
