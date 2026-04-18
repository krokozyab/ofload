package app.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Timer
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import java.util.concurrent.ThreadPoolExecutor

/**
 * Central metrics registry for the ETL service. Exposes a Prometheus-compatible
 * scrape endpoint at `GET /metrics` (see Routes.kt).
 *
 * Design:
 *   - One process-wide [PrometheusMeterRegistry].
 *   - Helper methods are preferred over callers touching the registry directly so
 *     metric names/labels are declared in one place.
 *   - Name convention: `etl.<area>.<metric>` — Micrometer auto-translates dots to
 *     underscores for the Prometheus output (`etl_area_metric`).
 *
 * **Metrics exposed**:
 *   - **Counters**: `etl_runs_total{target,status}`, `etl_rows_loaded_total{target}`,
 *     `etl_rows_merged_total{target}`, `etl_source_auth_retries_total{target}`,
 *     `etl_runner_rejections_total`.
 *   - **Timers** (exposed as `_seconds_count`/`_seconds_sum`/bucket histograms):
 *     `etl_page_duration_seconds{target}`, `etl_merge_duration_seconds{target}`,
 *     `etl_run_duration_seconds{target,status}`, `etl_source_fetch_seconds{target}`.
 *   - **Gauges**: `etl_pool_active{pool}`, `etl_pool_queue{pool}`.
 */
object Metrics {
    val registry: PrometheusMeterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    // ── Counters ──────────────────────────────────────────────

    /** Increment on final outcome of a target run. Status is "ok" or "failed". */
    fun runCompleted(target: String, status: String) =
        Counter.builder("etl.runs")
            .description("Total number of ETL runs by target and final status")
            .tags("target", target, "status", status)
            .register(registry)
            .increment()

    /** Rows staged into STG_* for this target (sum across pages). */
    fun rowsLoaded(target: String, rows: Long) =
        Counter.builder("etl.rows.loaded")
            .description("Total rows streamed from source into staging")
            .tags("target", target)
            .register(registry)
            .increment(rows.toDouble())

    /** Rows merged from STG_* into T_* for this target (sum across pages). */
    fun rowsMerged(target: String, rows: Long) =
        Counter.builder("etl.rows.merged")
            .description("Total rows affected by MERGE into the final target table")
            .tags("target", target)
            .register(registry)
            .increment(rows.toDouble())

    /** Incremented each time an auth-like source error triggered a reconnect/retry. */
    fun authRetry(target: String) =
        Counter.builder("etl.source.auth.retries")
            .description("Source-side auth/session failures that triggered a retry")
            .tags("target", target)
            .register(registry)
            .increment()

    /** Incremented when the RunManager queue overflowed and a /run* call returned 503. */
    fun runnerRejection() =
        Counter.builder("etl.runner.rejections")
            .description("HTTP submissions rejected by the bounded runner queue (AbortPolicy)")
            .register(registry)
            .increment()

    // ── Timers ────────────────────────────────────────────────

    /** Time spent on one full page (source stream + staging insert). */
    fun pageTimer(target: String): Timer =
        Timer.builder("etl.page.duration")
            .description("Duration of one full page (source read + staging insert)")
            .tags("target", target)
            .publishPercentileHistogram()
            .register(registry)

    /** Time spent on the MERGE STG → T step for one page. */
    fun mergeTimer(target: String): Timer =
        Timer.builder("etl.merge.duration")
            .description("Duration of the MERGE step per page")
            .tags("target", target)
            .publishPercentileHistogram()
            .register(registry)

    /** Time spent on one complete run (begin → finishRunOk|failRun). */
    fun runTimer(target: String, status: String): Timer =
        Timer.builder("etl.run.duration")
            .description("Duration of a complete target run by final status")
            .tags("target", target, "status", status)
            .publishPercentileHistogram()
            .register(registry)

    // ── Gauges ────────────────────────────────────────────────

    /**
     * Register active/queue gauges for a [ThreadPoolExecutor]. `poolName` distinguishes
     * the two pools in the service: `runner` (RunManager — HTTP submissions) and
     * `etl` (Pipeline — parallel target workers).
     *
     * Micrometer reads via the supplier on each scrape — cheap, non-blocking.
     */
    fun registerPoolGauges(poolName: String, pool: ThreadPoolExecutor) {
        Gauge.builder("etl.pool.active") { pool.activeCount.toDouble() }
            .description("Active threads in the thread pool")
            .tags("pool", poolName)
            .register(registry)
        Gauge.builder("etl.pool.queue") { pool.queue.size.toDouble() }
            .description("Pending tasks in the thread pool's queue")
            .tags("pool", poolName)
            .register(registry)
    }

    /** Prometheus text-format output for the `/metrics` endpoint. */
    fun scrape(): String = registry.scrape()
}
