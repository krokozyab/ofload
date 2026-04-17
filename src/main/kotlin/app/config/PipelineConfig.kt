package app.config

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.InputStream

/**
 * Declarative description of a single ETL target: what to pull from the Fusion
 * source, where to stage it in ATP, and how to merge it into the final table.
 *
 * All SQL is held verbatim in the config (sourceQuery, targetInsert, targetMerge)
 * so the pipeline performs no SQL generation — what you read in targets.json is
 * literally what runs against the DB. DDL for staging/target tables is applied
 * manually (files in src/main/resources/ddl/) and is NOT part of this config.
 *
 * @property name unique identifier used for logging, watermark lookup and API paths.
 * @property stagingTable ATP table that receives raw source rows (typically STG_*).
 *   Truncated at the start of every run. Columns must match targetInsert's column list.
 * @property targetTable final ATP table populated by targetMerge from staging.
 * @property watermarkColumn column in both source and staging that tracks the high-water mark
 *   for incremental loads. The sourceQuery must reference it as `watermarkColumn > ?`.
 * @property watermarkType "TIMESTAMP" or "NUMBER" — drives how the watermark value is parsed/compared.
 * @property keyColumns columns that uniquely identify a row. Used by tuple pagination
 *   (ORDER BY/WHERE key-tuple compare) and by targetMerge's ON clause. May be composite.
 * @property sourceQuery SELECT against Fusion. Must end with `WHERE <watermarkColumn> > ?`
 *   so the loader can bind the current watermark. Column list must match targetInsert.
 * @property targetInsert Parameterised INSERT into stagingTable. One `?` per source column,
 *   in the same order as the SELECT list. Used by StagingLoader as-is.
 * @property targetMerge Full MERGE statement from stagingTable into targetTable. Executed
 *   after staging is populated. The pipeline does no rewriting — it's the raw SQL.
 * @property initialWm watermark value for the first-ever run (before any ETL_WATERMARK row exists).
 * @property pageSize optional page size for tuple-paginated loads. Null = one-shot query, no pagination.
 * @property lookbackMinutes optional — on the first page of each run, subtract this many minutes
 *   from the persisted watermark to re-scan recently-updated rows. Guards against source-side
 *   clock skew and late-arriving updates.
 * @property group optional — targets sharing a group id are run in parallel in a single
 *   Fusion SSO session (see Pipeline.runTargets). Null = run alone.
 * @property nlsSettings optional map of `NLS_*` session params to apply on the staging
 *   connection before inserting (e.g. NLS_DATE_FORMAT). Needed when staging columns are
 *   typed (DATE/NUMBER) and source delivers strings.
 */
@Serializable
data class TargetConfig(
    val name: String,
    val stagingTable: String,
    val targetTable: String,
    val watermarkColumn: String,
    val watermarkType: String,       // "TIMESTAMP" | "NUMBER"
    val keyColumns: List<String>,
    val sourceQuery: String,
    val targetInsert: String,
    val targetMerge: String,
    val initialWm: String,
    val pageSize: Int? = null,
    val lookbackMinutes: Long? = null,
    val group: Int? = null,
    val nlsSettings: Map<String, String>? = null
)

/**
 * Root of targets.json: a flat list of [TargetConfig]s. Loaded once at startup and
 * held immutably in ConfigHolder — the file is hand-edited, not written by the app.
 */
@Serializable
data class PipelineConfig(val targets: List<TargetConfig>)

/**
 * Deserialise targets.json from the given stream. Called once from Main with the
 * classpath resource stream. No schema validation beyond what kotlinx.serialization
 * enforces — missing required fields or malformed JSON will throw here and abort startup.
 */
fun loadConfig(stream: InputStream): PipelineConfig =
    Json.decodeFromString(PipelineConfig.serializer(), stream.bufferedReader().readText())
