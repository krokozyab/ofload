# Environment variables

## Application (ofload)

| Variable | Default | Purpose |
|---|---|---|
| `DB_USER` | — (required) | ATP user |
| `DB_PASSWORD` | — (required) | ATP password |
| `DB_CONNECT_STRING` | — (required) | TNS descriptor or connect string |
| `TNS_ADMIN` | — (optional) | Directory containing wallet for mTLS; when set the URL includes `?TNS_ADMIN=…` |
| `SOURCE_URL` | — (required) | Fusion JDBC URL (e.g. `jdbc:wsdl://your-host.oraclecloud.com`) |
| `SOURCE_AUTH_TYPE` | — (optional) | One of `BROWSER`, `BASIC`, `BEARER`. Unset ⇒ driver falls back to its legacy BASIC-by-default branch (requires `SOURCE_USER`+`SOURCE_PASSWORD`). Set explicitly when you want SSO (`BROWSER`) or raw-token auth (`BEARER`). |
| `SOURCE_USER` | conditional | Required for `BASIC` (and the legacy default). Unused for `BROWSER`/`BEARER`. |
| `SOURCE_PASSWORD` | conditional | Password for `BASIC`, raw JWT for `BEARER`. Unused for `BROWSER`. |
| `SOURCE_SSO_TIMEOUT` | `300` (driver default) | Seconds to wait for the browser SSO flow. Applies only when `SOURCE_AUTH_TYPE=BROWSER`. |
| `SOURCE_DRIVER_CLASS` | — (required) | ofjdbc driver class name (`my.jdbc.wsdl_driver.WsdlDriver`) |
| `SOURCE_QUERY_TIMEOUT_SECONDS` | `600` | Upper bound per page fetch (driver's per-request read timeout and retry logic work underneath this; see below) |
| `PORT` | `8080` | HTTP port for dashboard/API |
| `RUNNER_THREADS` | `4` | Concurrent HTTP `/run*` submissions accepted by `RunManager` |
| `RUNNER_QUEUE_SIZE` | `16` | Pending submissions before `/run*` returns 503 |
| `ETL_WORKERS` | `8` | Concurrent target workers in `Pipeline.etlPool` |
| `ETL_QUEUE_SIZE` | `32` | Pending inner submissions before `CallerRunsPolicy` kicks in |
| `TARGET_ROW_COUNT_TTL_SECONDS` | `60` | TTL for the cached `SELECT COUNT(*) FROM <targetTable>` values shown on `/status`. Refresh happens asynchronously — the HTTP handler never blocks on COUNT. Lower it for faster dashboard updates on small targets; raise it to reduce load on large tables. |

## Source driver (ofjdbc)

These are read by the driver directly, not by this app. Setting them tunes the
underlying source-side behaviour.

### HTTP transport
| Variable | Default | Notes |
|---|---|---|
| `OFJDBC_HTTP_CONNECT_TIMEOUT` | `30` (seconds) | TCP connect timeout to BI Publisher. Range 1–300. |
| `OFJDBC_HTTP_READ_TIMEOUT` | `120` (seconds) | Per-request read timeout — how long to wait for a single WSDL response. Range 5–1800. |

### Per-request retry (inside the driver, transparent to us)
| Variable | Default | Notes |
|---|---|---|
| `OFJDBC_RETRY_MAX_ATTEMPTS` | `3` | How many attempts before giving up on a single HTTP round-trip. |
| `OFJDBC_RETRY_BASE_DELAY_MS` | `1000` | First backoff delay. |
| `OFJDBC_RETRY_MAX_DELAY_MS` | `30000` | Cap on backoff. |
| `OFJDBC_RETRY_MULTIPLIER` | `2.0` | Exponential factor. |

Retry fires on `HttpTimeoutException`, `ConnectException`, connection-reset /
connection-refused / network-unreachable errors. Does **not** retry on `ORA-*`
errors (permanent).

### Connection properties (set via JDBC `Properties`, not env vars)
Already wired in `SourceDs.openConnection`:

| Property | Value | Reason |
|---|---|---|
| `ofjdbc.prefetch.enabled` | `false` | Our ETL uses a Java-side pageSize cap + `rs.close()`; the driver's 75% lookahead-prefetch would fire a redundant WSDL round-trip every page that gets cancelled. |

## How the timeout layers stack

For a single page fetch (`StagingLoader.load` → `rs.next()` loop):

```
┌─────────────────────────────────────────────────────────────────────┐
│ srcPs.queryTimeout = SOURCE_QUERY_TIMEOUT_SECONDS (default 600 s)    │  ← our cap
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │ PaginatedResultSet.fetchPageXml — retry loop                 │   │
│  │   up to OFJDBC_RETRY_MAX_ATTEMPTS with exponential backoff   │   │
│  │  ┌────────────────────────────────────────────────────────┐  │   │
│  │  │ One WSDL HTTP request:                                 │  │   │
│  │  │   connect ≤ OFJDBC_HTTP_CONNECT_TIMEOUT (30 s)         │  │   │
│  │  │   read    ≤ OFJDBC_HTTP_READ_TIMEOUT    (120 s)        │  │   │
│  │  └────────────────────────────────────────────────────────┘  │   │
│  │   ← retry on transient failures, re-check outer deadline     │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

Inner timeouts fire first (transport-level). Driver retries transient failures
with backoff, re-checking the outer deadline between attempts. Our cap only
triggers if the entire cascade stays stuck for longer than its budget.

Worst realistic wall-clock budget under defaults:
```
3 attempts × (120 s read + ~30 s backoff) = ~450 s    → still < 600 s cap
```

Tune `SOURCE_QUERY_TIMEOUT_SECONDS` upward if you have pages that legitimately
run longer than 10 minutes; downward if you want faster fail-fast at the cost of
giving up on slow-but-progressing queries.

## Metrics — `GET /metrics`

Prometheus-compatible scrape endpoint. Polled by Prometheus / VictoriaMetrics /
Grafana Agent every 15–60 s. Content-Type is `text/plain; version=0.0.4`.

### Counters
| Metric | Labels | Meaning |
|---|---|---|
| `etl_runs_total` | `target`, `status` ("ok"/"failed") | Completed runs, grouped by outcome. |
| `etl_rows_loaded_total` | `target` | Rows streamed from source into staging. |
| `etl_rows_merged_total` | `target` | Rows affected by MERGE into the target table. |
| `etl_source_auth_retries_total` | `target` | Source-side auth failures that triggered reconnect. |
| `etl_runner_rejections_total` | — | HTTP `/run*` calls rejected (queue full → 503). |

### Timers (histograms)
Each emits `_seconds_count`, `_seconds_sum`, and percentile buckets.

| Metric | Labels | Meaning |
|---|---|---|
| `etl_page_duration_seconds` | `target` | Per-page source read + staging insert duration. |
| `etl_merge_duration_seconds` | `target` | MERGE STG → T duration per page. |
| `etl_run_duration_seconds` | `target`, `status` | Total run duration (from beginRun to finishRunOk/failRun). |

### Gauges
| Metric | Labels | Meaning |
|---|---|---|
| `etl_pool_active` | `pool` ("runner"/"etl") | Active threads in each pool. |
| `etl_pool_queue` | `pool` | Pending tasks queued in each pool. |

Plus Micrometer's default JVM metrics once we enable them (heap, GC, threads) —
not yet wired; add via `new JvmMemoryMetrics().bindTo(registry)` etc. if needed.

### Example alerts (PromQL)
```promql
# Failure rate > 10% in the last 15 min
sum(rate(etl_runs_total{status="failed"}[15m])) by (target)
  /
sum(rate(etl_runs_total[15m])) by (target)
  > 0.1

# Runner queue filling up (load balancer / backpressure warning)
etl_pool_queue{pool="runner"} > 10

# p95 page duration > 60 s (source degradation)
histogram_quantile(0.95, rate(etl_page_duration_seconds_bucket[5m])) > 60

# No successful run for 1h on a target (missing heartbeat)
time() - max by (target) (etl_runs_total{status="ok"}) > 3600
```
