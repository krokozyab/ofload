package app.etl

import app.config.TargetConfig
import java.sql.Connection

/**
 * Executes the MERGE step that moves rows from staging into the final target table.
 *
 * There is no SQL generation here — the MERGE statement is held verbatim in
 * [TargetConfig.targetMerge] and executed as-is. This keeps the logic transparent
 * (exactly what you read in targets.json is what runs) and lets operators customise
 * MATCHED/NOT-MATCHED clauses per target without touching Kotlin code.
 */
object TargetMerger {
    /**
     * Run the configured MERGE and return the number of affected rows.
     * The row count is advisory (Oracle counts both inserted and updated rows
     * together) and used only for logging in [Pipeline].
     */
    fun merge(conn: Connection, target: TargetConfig): Int =
        conn.createStatement().use { it.executeUpdate(target.targetMerge) }
}
