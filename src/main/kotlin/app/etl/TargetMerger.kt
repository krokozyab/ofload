package app.etl

import app.config.TargetConfig
import java.sql.Connection

object TargetMerger {
    fun merge(conn: Connection, target: TargetConfig): Int =
        conn.createStatement().use { it.executeUpdate(target.targetMerge) }
}
