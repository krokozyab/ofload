package app.db

import java.sql.Connection
import java.sql.DriverManager

object SourceDs {
    init {
        val driverClass = System.getenv("SOURCE_DRIVER_CLASS") ?: error("SOURCE_DRIVER_CLASS not set")
        Class.forName(driverClass)
    }

    fun getConnection(): Connection {
        val url = System.getenv("SOURCE_URL") ?: error("SOURCE_URL not set")
        return DriverManager.getConnection(url, System.getenv("SOURCE_USER"), System.getenv("SOURCE_PASSWORD"))
    }
}
