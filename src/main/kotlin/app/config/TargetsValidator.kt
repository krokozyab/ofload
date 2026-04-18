package app.config

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Single validation finding — a message tied to a specific target (or `null` for
 * global-scope issues like duplicate names).
 */
data class ValidationError(val target: String?, val message: String)

/**
 * Fail-fast static validation of the parsed `targets.json`. Called once at service
 * startup (see Main) before the Pipeline and HTTP server come up.
 *
 * Catches the configuration mistakes that otherwise only surface on the first run:
 *   - `/*WM*/` marker missing from `sourceQuery` — pagination would break silently.
 *   - SELECT column count not matching `targetInsert` placeholder / column count —
 *     first row fails mid-pipeline with a confusing "Parameter X missing" error.
 *   - `watermarkColumn` or `keyColumns` not referenced in the SELECT list — runtime
 *     `ResultSet.findColumn` throws inside the streaming loop.
 *   - `targetMerge` forgetting to reference its staging/target tables — MERGE
 *     silently affects zero rows.
 *   - `initialWm` not parseable for its declared `watermarkType` — first run crashes.
 *   - duplicate `name`, invalid `watermarkType`, non-positive `pageSize`, negative
 *     `lookbackMinutes`.
 *
 * **Not covered** (requires a DB round-trip — separate validator class): existence
 * of staging/target tables in ATP, column-type compatibility, privileges.
 */
object TargetsValidator {

    /** Validate a loaded config. Returns all findings at once so the user sees the full picture. */
    fun validate(config: PipelineConfig): List<ValidationError> {
        val errors = mutableListOf<ValidationError>()

        // Global: name uniqueness.
        config.targets
            .groupBy { it.name }
            .filter { it.value.size > 1 }
            .forEach { (name, _) ->
                errors += ValidationError(null, "duplicate target name: '$name'")
            }

        for (t in config.targets) validateOne(t, errors)
        return errors
    }

    private fun validateOne(t: TargetConfig, errors: MutableList<ValidationError>) {
        fun err(msg: String) { errors += ValidationError(t.name, msg) }

        // ── watermarkType / initialWm ─────────────────────────────────
        when (t.watermarkType) {
            "TIMESTAMP" -> {
                val fmt = DateTimeFormatter.ofPattern(
                    "yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]"
                )
                runCatching { LocalDateTime.parse(t.initialWm, fmt) }.onFailure {
                    err("initialWm '${t.initialWm}' does not parse as TIMESTAMP " +
                        "(expected 'yyyy-MM-dd HH:mm:ss[.fraction]')")
                }
            }
            "NUMBER" -> {
                if (t.initialWm.toBigDecimalOrNull() == null) {
                    err("initialWm '${t.initialWm}' is not a valid number for watermarkType=NUMBER")
                }
            }
            else -> err("watermarkType must be 'TIMESTAMP' or 'NUMBER' (got '${t.watermarkType}')")
        }

        // ── keyColumns / pageSize / lookback ─────────────────────────
        if (t.keyColumns.isEmpty()) err("keyColumns must contain at least one column")
        if (t.pageSize != null && t.pageSize <= 0) err("pageSize must be positive (got ${t.pageSize})")
        if (t.lookbackMinutes != null && t.lookbackMinutes < 0) {
            err("lookbackMinutes must be >= 0 (got ${t.lookbackMinutes})")
        }

        // ── sourceQuery ───────────────────────────────────────────────
        val srcUpper = t.sourceQuery.uppercase()
        if (!srcUpper.trimStart().startsWith("SELECT")) {
            err("sourceQuery must begin with SELECT")
        }
        if (!t.sourceQuery.contains("/*WM*/")) {
            err("sourceQuery must contain '/*WM*/' marker where the watermark predicate will be injected")
        }

        val selectCols = extractSelectColumns(t.sourceQuery)
        if (selectCols == null) {
            err("could not parse SELECT column list from sourceQuery (missing FROM?)")
        } else {
            val selectUpper = selectCols.map { it.uppercase() }.toSet()
            if (t.watermarkColumn.uppercase() !in selectUpper) {
                err("watermarkColumn '${t.watermarkColumn}' is not in sourceQuery SELECT list")
            }
            t.keyColumns.forEach { k ->
                if (k.uppercase() !in selectUpper) {
                    err("keyColumn '$k' is not in sourceQuery SELECT list")
                }
            }
        }

        // ── targetInsert ──────────────────────────────────────────────
        val insertUpper = t.targetInsert.uppercase()
        if ("INSERT" !in insertUpper) err("targetInsert must be an INSERT statement")
        if (t.stagingTable.uppercase() !in insertUpper) {
            err("targetInsert should reference stagingTable '${t.stagingTable}'")
        }

        val insertCols = extractInsertColumns(t.targetInsert)
        val placeholderCount = t.targetInsert.count { it == '?' }
        if (insertCols == null) {
            err("could not parse column list from targetInsert")
        } else {
            if (insertCols.size != placeholderCount) {
                err("targetInsert placeholder count ($placeholderCount) does not match " +
                    "column list size (${insertCols.size})")
            }
            if (selectCols != null && insertCols.size != selectCols.size) {
                err("targetInsert column count (${insertCols.size}) does not match " +
                    "sourceQuery SELECT count (${selectCols.size}) — will crash on first row")
            }
        }

        // ── targetMerge ───────────────────────────────────────────────
        val mergeUpper = t.targetMerge.uppercase()
        if ("MERGE" !in mergeUpper) err("targetMerge must be a MERGE statement")
        if (t.stagingTable.uppercase() !in mergeUpper) {
            err("targetMerge should reference stagingTable '${t.stagingTable}'")
        }
        if (t.targetTable.uppercase() !in mergeUpper) {
            err("targetMerge should reference targetTable '${t.targetTable}'")
        }
    }

    /**
     * Extract the column list from a `SELECT col1, col2, ... FROM ...` statement.
     * Strips identifiers of surrounding whitespace; does NOT resolve aliases or
     * expressions. Returns null when the query doesn't look like a plain SELECT.
     */
    internal fun extractSelectColumns(sql: String): List<String>? {
        val upper = sql.uppercase()
        val selectAt = upper.indexOf("SELECT")
        if (selectAt < 0) return null
        val fromAt = upper.indexOf(" FROM ", selectAt)
        if (fromAt < 0) return null
        val colsBlock = sql.substring(selectAt + "SELECT".length, fromAt).trim()
        if (colsBlock.isEmpty()) return null
        return colsBlock.split(",").map { it.trim() }
    }

    /**
     * Extract the explicit column list from an `INSERT [hint] INTO table (col1, col2, ...) VALUES (?, ?, ...)`
     * statement. Returns null when no parenthesised column list is present (e.g. a bare
     * `INSERT INTO t VALUES (...)` — valid SQL but rejected by our validator).
     */
    internal fun extractInsertColumns(sql: String): List<String>? {
        val regex = Regex(
            """INSERT\s+(?:/\*.*?\*/\s+)?INTO\s+\S+\s*\(([^)]+)\)""",
            setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL)
        )
        val match = regex.find(sql) ?: return null
        return match.groupValues[1].split(",").map { it.trim() }
    }
}
