# Operations

Everything you need to deploy, observe, and troubleshoot `ofload` in
production-adjacent environments.

## Deployment

### Where this runs and why it's cheap

`ofload` is a **single JVM process with bounded memory**. Thanks to streaming
(constant `BATCH_SIZE` rows in flight) and bounded thread pools, RAM usage
stays flat regardless of table size — million-row tables consume the same
~256 MB heap as thousand-row tables.

That makes it comfortable to run on essentially any small compute:

| Platform | Suitable shape | Approx cost |
|---|---|---|
| **OCI Always Free** | VM.Standard.E2.1.Micro (1 OCPU / 1 GB) or Ampere A1 Flex (4 OCPU / 24 GB free tier) | **$0** |
| **OCI paid** | VM.Standard.E4.Flex 1 OCPU / 4–8 GB | ~$20–30/mo |
| **AWS** | t4g.small (2 vCPU / 2 GB) | ~$15/mo |
| **Azure** | B2s (2 vCPU / 4 GB) | ~$30/mo |
| **DigitalOcean / Hetzner** | 1 vCPU / 1–2 GB droplet | $5–10/mo |
| **On-prem Linux box** | Anything that fits `java -jar` | free (existing hardware) |
| **Kubernetes pod** | 250m CPU / 512 MB RAM request | whatever your cluster costs |

The JAR is ~40 MB with all dependencies (Oracle JDBC + ofjdbc + Javalin +
Micrometer + Kotlin stdlib). Docker image lands under 200 MB on top of
`eclipse-temurin:21-jre`. Cold start is 2–3 s.

**Why it's so cheap compared to the alternatives:**

| Cost component | OIC-based pipeline | BICC-based pipeline | `ofload` |
|---|---|---|---|
| Compute for the orchestrator | Included in OIC bill | Separate (ODI / OCI DI) | One small VM |
| Per-message / per-row licensing | **Yes** — scales with data volume | — | **None** |
| Object Storage for intermediate files | — | **Yes** — $0.023/GB/mo + egress | **None** (direct JDBC → MERGE) |
| Downstream loader licensing | — | **Yes** — ODI or OCI DI | — |
| Number of deployed artifacts per table | 1 BIP report + 1 flow + 1 tracker | 1 VO config + bucket rule | **1 JSON entry** |
| Scheduler infrastructure | Included | Separate | External (cron/k8s/OIC — your choice) |

A typical deployment footprint:

- **Compute**: 1 small VM or one k8s pod. $0–30/mo.
- **Fusion access**: your existing Fusion licensing (no additional cost).
- **ATP**: your existing Autonomous Database (no additional cost — the target
  DB you already pay for).
- **ofjdbc**: open source, no licensing.
- **ofload**: open source, no licensing.

For an organisation already running Fusion + ATP, **the marginal cost of
adding `ofload` is effectively just the small VM it runs on**. The
alternative (OIC message packs for tens of millions of rows/month, or BICC
object storage + ODI licensing) is routinely orders of magnitude more.

### Single JAR

`./gradlew shadowJar` produces `build/libs/ofload-1.0-SNAPSHOT-all.jar` —
a fat JAR containing all dependencies including the ofjdbc driver. The
minimal run command is:

```bash
java -jar build/libs/ofload-1.0-SNAPSHOT-all.jar
```

All configuration is env-var driven — see [env-vars.md](env-vars.md).

### Docker

There is no `Dockerfile` in the repo yet; a minimal one looks like:

```dockerfile
FROM eclipse-temurin:21-jre
COPY build/libs/ofload-1.0-SNAPSHOT-all.jar /app/ofload.jar
# Mount wallet at /wallet; TNS_ADMIN points there.
VOLUME /wallet
ENV TNS_ADMIN=/wallet
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/ofload.jar"]
```

Pass credentials at runtime with `--env-file`, Kubernetes secrets, or
OCI Vault — never bake them into the image.

### Kubernetes

Two probe endpoints to wire up:

```yaml
livenessProbe:
  httpGet: { path: /live, port: 8080 }
  initialDelaySeconds: 10
readinessProbe:
  httpGet: { path: /ready, port: 8080 }
  initialDelaySeconds: 30
  periodSeconds: 15
```

- `/live` never touches the DB — it just confirms the JVM is answering HTTP.
  Failures here restart the pod.
- `/ready` pings ATP (`SELECT 1 FROM DUAL`) and verifies the source driver
  class is loadable. Failures here take the pod out of service rotation but
  don't restart — gives transient DB blips a chance to recover.

Apply schema migrations before starting the service, not in parallel across
replicas. For multi-replica deployments, either:

- Have one canonical "migrator" init container that runs first, then start
  regular replicas.
- Or rely on the startup migrator being idempotent (it is) and accept that
  concurrent first-boots may race briefly — Oracle's CREATE TABLE is serialised
  by the data dictionary, so one wins and the others swallow ORA-00955.

### Systemd (single VM)

```ini
[Unit]
Description=ofload ETL service
After=network.target

[Service]
Type=simple
User=ofload
EnvironmentFile=/etc/ofload/env
ExecStart=/usr/bin/java -jar /opt/ofload/ofload.jar
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Graceful shutdown (`SIGTERM`) is honoured — the service:

1. Stops accepting new HTTP `/run*` submissions.
2. Waits up to 5 minutes for in-flight runs to finish.
3. Closes the row-count cache background thread.
4. Shuts the ETL worker pool with another 5-minute grace.
5. Tears down the source connection (if any) and the UCP pool.

A final `SIGKILL` kicks in if the grace period elapses — no data loss,
because every page commits before advancing the watermark.

### Scheduling runs — REST is the only control plane

`ofload` has no internal scheduler. Every run — production, demo, ad-hoc —
goes through HTTP. This is by design: scheduling policy belongs in the system
your org already uses for cron/pipelines.

**Core endpoints:**

| Method | Path | Body | Response |
|---|---|---|---|
| POST | `/run` | — | 202 `{runId, targets, skipped, message}` / 409 if all running / 503 if queue full |
| POST | `/run/group/{id}` | — | 202 / 404 if group has no targets / 409 / 503 |
| POST | `/run/target/{name}` | — | 202 / 404 if target unknown / 409 / 503 |
| GET | `/status` | — | 200 `{targets: [{name, running, watermark, targetRowCount}]}` |

Non-blocking: every POST returns within milliseconds and the actual ETL runs
on a background thread pool. Orchestrators that need to wait for completion
poll `/status` until the target leaves `RUNNING` state (either by checking
`targets[i].running == false`, or by comparing `watermark.currentRunStarted
== null`, or by correlating `watermark.currentRunId` against the POST's
returned `runId`).

**Typical integrations:**

- **Cron on the host** — `*/15 * * * * curl -fsS -X POST http://ofload:8080/run`.
- **Kubernetes CronJob** — a tiny `curl` image hitting the ofload service on the cluster network.
- **OIC scheduled integration** — a scheduled flow whose sole action is `POST /run`. OIC keeps scheduling, fault routing, notifications; `ofload` does the SQL work.
- **Airflow / Dagster / Prefect** — one HTTP task per target or per group.
- **Jenkins / GitLab CI** — `curl` step in a pipeline.
- **OCI Scheduler** — HTTP invocation target.

**Concurrency contract:**

- A POST for a target already running returns `{targets: [], skipped: [name], message: "... already running"}` with HTTP 409. Safe to call repeatedly from an external scheduler — it won't double-run.
- A POST when the runner queue is full returns HTTP 503 `{error: "Runner queue full — try again later"}`. Scheduler should back off and retry.
- Runs kicked off by different endpoints (one `POST /run/target/foo`, then a `POST /run/group/X` that includes `foo`) dedupe — the second call skips `foo`.

### OIC integration example

This is the most common real-world driver for `ofload`: an OIC scheduled
integration triggers the pipeline, optionally polls for completion, and uses
OIC's native fault routing / alerting. `ofload` is the worker; OIC stays the
orchestrator.

#### 1. Register ofload as a REST connection

**Design → Connections → Create → REST adapter.**

| Field | Value |
|---|---|
| Role | Invoke |
| Connection URL | `http://<ofload-host>:8080` |
| Security Policy | No Security Policy (if inside private network), or Basic Auth / OAuth — `ofload` itself has no HTTP auth, so put the service behind a gateway if exposed publicly |

Test the connection.

#### 2. Create a scheduled integration

**Design → Integrations → Create → Scheduled Orchestration.**

Name: `Sync_Fusion_to_Warehouse_Hourly` (or whatever suits).

#### 3. Add an "Invoke" action calling ofload

Drag the REST connection from step 1 onto the canvas. Configure the endpoint:

| Field | Value |
|---|---|
| Endpoint relative URI | `/run/group/1` (or `/run`, or `/run/target/{name}`) |
| HTTP method | `POST` |
| Request body | None |
| Configure response | Yes — JSON |
| Sample response | Paste the sample JSON below |

Sample response payload for the "Configure Response" step (lets OIC build
the response schema):

```json
{
  "runId": "abc-123-def",
  "targets": ["gl_je_headers", "ap_invoices"],
  "skipped": [],
  "message": "Group 1 started"
}
```

#### 4. Fire-and-forget — don't poll

OIC integrations have a hard 5-minute execution timeout; full ETL runs often
take 20+ minutes. Do **not** add a polling loop — end the integration
immediately after the 202 response. `ofload` continues running in the
background regardless of HTTP connection closure; check completion via
`/status`, the dashboard, Prometheus, or `ETL_WATERMARK` whenever you want.

#### 5. Schedule it

**Actions menu → Schedule → Add schedule.** Pick cadence (every 15 min, every
hour, daily at 02:00 — whatever matches your target freshness SLA).

Activate the integration. OIC's scheduler will now POST `/run/group/1` on the
chosen cadence.

#### 6. OIC-side fault handling

In the scope's **Fault Handler** branch, route on HTTP status code:

| Status from ofload | Meaning | Recommended OIC action |
|---|---|---|
| 202 Accepted | Run started successfully | Continue |
| 409 Conflict | A run is already in progress | Swallow (nothing to do) or log and continue |
| 503 Service Unavailable | Runner queue full | Re-throw / alert / schedule retry via OIC's retry policy |
| 4xx other | Client error (unknown target, invalid URL) | Alert — config drift between OIC and ofload |
| 5xx other | ofload internal error | Alert — fetch `/status` for details |

#### 7. Verifying from OIC's side

OIC's **Monitoring → Tracking** shows every scheduled run with the response
payload, including `ofload`'s `runId`. That `runId` appears in ofload's
structured logs (`MDC.runId`) and in ETL_WATERMARK (`CURRENT_RUN_ID` /
historical equivalents) — so you can correlate across both systems:

```sql
SELECT TARGET_NAME, CURRENT_RUN_ID, CURRENT_RUN_STARTED,
       LAST_SUCCESS_FINISHED, LAST_FAILURE_ERROR
  FROM ETL_WATERMARK
 WHERE CURRENT_RUN_ID = 'abc-123-def'
    OR TARGET_NAME = 'gl_je_headers';
```

#### What OIC still brings to the table

After this setup, OIC owns:

- **Scheduling** — cron-like cadence, holiday exclusions, SLA-aware timing.
- **Notifications** — email / SMS / Slack on fault.
- **Approval gates** — human-in-the-loop before critical runs.
- **Chaining** — trigger downstream integrations after `ofload` completes.
- **Audit trail** — activity log with payloads.

`ofload` owns the actual SQL extraction, pagination, watermark, MERGE, retry,
and metrics — the parts OIC historically does poorly for large Fusion pulls.

## Observability

### Prometheus

`GET /metrics` returns standard Prometheus text format. Point a Prometheus
server at it with a 15–60 s scrape interval.

Full metric catalogue is in [env-vars.md](env-vars.md#metrics---get-metrics).
Headlines:

- `etl_runs_total{target,status}` — success/failure rate per target.
- `etl_rows_loaded_total{target}` / `etl_rows_merged_total{target}` — throughput.
- `etl_page_duration_seconds_bucket{target,le=…}` — page latency histogram.
- `etl_run_duration_seconds_bucket{target,status,le=…}` — end-to-end run time.
- `etl_source_auth_retries_total{target}` — how often token refresh fired.
- `etl_runner_rejections_total` — HTTP submissions hitting the queue ceiling.
- `etl_pool_active{pool}` / `etl_pool_queue{pool}` — live pool pressure.

### Recommended alerts

```promql
# Any target failing in the last 15 min
increase(etl_runs_total{status="failed"}[15m]) > 0

# p95 page duration regressed above 60 s (source slow / degraded)
histogram_quantile(0.95, rate(etl_page_duration_seconds_bucket[5m])) > 60

# Runner queue filling — operator is hitting the service too fast
etl_pool_queue{pool="runner"} > 10

# Target hasn't succeeded in > 1 h (missing heartbeat / upstream broken)
(time() - max by (target) (etl_runs_total{status="ok"})) > 3600

# Excessive auth retries — token cache may be misbehaving
rate(etl_source_auth_retries_total[15m]) > 0.1
```

### Grafana dashboard

A minimal 6-panel dashboard answers most operational questions:

1. **Throughput** — `sum by (target) (rate(etl_rows_merged_total[5m]))` as stacked lines.
2. **Run success rate** — `sum by (target, status) (rate(etl_runs_total[15m]))`.
3. **Page latency p50/p95** — `histogram_quantile(0.5|0.95, rate(etl_page_duration_seconds_bucket[5m]))`.
4. **Run latency per target** — `histogram_quantile(0.95, rate(etl_run_duration_seconds_bucket{status="ok"}[15m]))` faceted by target.
5. **Pool pressure** — `etl_pool_active` / `etl_pool_queue` as stacked area, faceted by `pool`.
6. **Recent failures** — table from `etl_runs_total{status="failed"}` with 24 h range.

### Structured logs

All logs use SLF4J with MDC context. Relevant fields:

- `target` — target name (present during any target-scoped work).
- `runId` — UUID from `POST /run*` response (present for the whole run).
- `page` — current page number.
- `group` — group id during parallel-group execution.

JSON encoder is wired via `logback-json.xml` — opt in for production by
pointing `LOGBACK_CONFIG=logback-json.xml`. Example query in a log aggregator:

```
ofload.target = "ap_invoices" AND ofload.runId = "abc-123-def"
```

pulls every line of a specific run out of mixed concurrent output.

### Health surface

| Endpoint | Purpose | Suitable for |
|---|---|---|
| `/live` | JVM is answering HTTP | k8s livenessProbe, restart triggers |
| `/ready` | ATP reachable + source driver loaded | k8s readinessProbe, load-balancer checks |
| `/health` | Alias of /ready for backward-compat | — (prefer /ready in new setups) |
| `/status` | Full per-target state JSON | Dashboards, humans |
| `/metrics` | Prometheus scrape | Monitoring stack |

## Authentication modes

| Mode | Selected by | Suitable for | Caveats |
|---|---|---|---|
| **BROWSER** | `SOURCE_USER`/`SOURCE_PASSWORD` unset | Dev / demo / interactive runs | Opens a browser on first auth and on token expiry — **do not use for cron**. |
| **BASIC** | `SOURCE_USER` + `SOURCE_PASSWORD` set | CI, simple service accounts | 401 is terminal (bad creds) — no auto-recovery, fails fast. |
| **BEARER** | Raw JWT in `SOURCE_PASSWORD` | Short-lived test harnesses | JWT expires in ~1 h; no refresh — must be supplied fresh. |
| **CLIENT_CREDENTIALS** | Future — see [proposals/](proposals/ofjdbc-client-credentials.md) | Production unattended | Needs driver-side support; roadmap item. |

For cron/scheduled production today, use **BASIC** with a dedicated Fusion
service account whose password rotates on a schedule you control.

## Troubleshooting

### `targets.json validation failed` on startup

The error block names each invalid field and the target it belongs to. Read
top-to-bottom — all issues are reported at once, not just the first.

Common cases:

| Error text | Fix |
|---|---|
| `must contain '/*WM*/' marker` | Add the literal `/*WM*/` to the SELECT's `WHERE` clause |
| `watermarkColumn '…' is not in sourceQuery SELECT list` | Add the column to the SELECT list |
| `targetInsert placeholder count … does not match column list size` | Count of `?` must equal count of columns in the INSERT column list |
| `targetInsert column count … does not match sourceQuery SELECT count` | Both column lists must be the same length and order |
| `initialWm '…' does not parse as TIMESTAMP` | Use `yyyy-MM-dd HH:mm:ss[.fraction]` form |

### `ORA-00904: "CURRENT_RUN_ROWS": invalid identifier` (or similar)

A schema migration hasn't been applied. This should be automatic on service
startup — if you're seeing it, the migrator didn't run or the user lacks
`ALTER TABLE`.

Manual fix:
```sql
-- Apply the file listed in the migrations manifest that adds the missing column.
-- e.g. src/main/resources/migrations/002_typed_watermark_columns.sql
```

Then restart the service.

### `Source returned more than pageSize=N rows` warning

(This message is informational in earlier code paths — the current version
enforces the cap on the Java side silently.) It means the source driver or
BI Publisher report returned more rows than the outer limit — the Java-side
`while (count < pageLimit)` break still caps correctly. Nothing to do unless
you see pages looping without the seek cursor advancing.

### Service hangs on "ATP connected" or "Source connection opened"

- ATP side: wallet misconfigured. Check `TNS_ADMIN` points to the **unzipped
  directory** (not the zip). Verify `tnsnames.ora` has the connect-string you
  passed in `DB_CONNECT_STRING`.
- Source side: SSO browser didn't open (headless environment) or Fusion host
  unreachable. Verify `SOURCE_URL`, check network ACLs.

### Run stuck in `RUNNING` forever after a crash

Shouldn't happen on a clean restart — `Migrator` runs `resetStaleRunning`
which flips any orphaned `CURRENT_RUN_STARTED != NULL` rows into
`LAST_FAILURE_*`. If you see it, check the startup log for errors in the
stale-reset step.

Manual fix:
```sql
UPDATE ETL_WATERMARK
   SET CURRENT_RUN_STARTED = NULL,
       CURRENT_RUN_ID = NULL,
       CURRENT_RUN_ROWS = 0,
       LAST_FAILURE_FINISHED = SYS_EXTRACT_UTC(SYSTIMESTAMP),
       LAST_FAILURE_ERROR = 'Manual reset after crash'
 WHERE CURRENT_RUN_STARTED IS NOT NULL;
COMMIT;
```

### `Runner queue full — try again later` (HTTP 503)

The `RunManager` bounded queue is full. Either:

- Too many concurrent `/run*` submissions — serialise the caller.
- Increase `RUNNER_QUEUE_SIZE` if 16 is genuinely too small for your cadence.
- Increase `RUNNER_THREADS` if workers are the bottleneck rather than queue.

Check `/metrics` → `etl_runner_rejections_total` for the rejection rate and
`etl_pool_queue{pool="runner"}` for live pressure.

### Page duration suddenly doubled

Check `etl_source_auth_retries_total` — if it jumped, the source token cache
has been churning (token expired, driver re-authed mid-page). One-off, no
action needed. Sustained → investigate Fusion session TTL or network
instability.

If no auth retries, the source is genuinely slow. Check BI Publisher report
health in Fusion itself.

### Dashboard shows no data / stale data

- Auto-refresh disabled — click `15s` in the top-right.
- `/status` polling broken — open browser devtools, check the network tab.
- `COUNT(*)` cache warming up — first call after startup returns `-` for
  `Total`; second call shows numbers. If it never populates, check ATP
  connectivity.

### Deadlock / MERGE contention

`T_*` MERGEs run on per-target connections, so no cross-target locking is
possible. If you see `ORA-00060` (deadlock detected) in `LAST_FAILURE_ERROR`,
it's self-contention from two workers processing the same target — which
shouldn't happen because `runningTargets` lock prevents overlapping runs
of the same target.

Investigate: check `LAST_FAILURE_ERROR` for the full `ORA-00060` trace, look
at `user_tab_modifications` and `v$lock` for concurrent writers outside
`ofload`.

## Wallet rotation

For ATP wallet:

1. Download new wallet, unzip into a new directory (e.g. `/wallet-new`).
2. Update `TNS_ADMIN=/wallet-new` in the service's env.
3. Restart the service — UCP picks up the new wallet on the next pool
   initialisation.

You can do this while runs are in flight — graceful shutdown waits for them.
Alternatively, rolling restarts with k8s handle it transparently.

## Upgrading `ofload`

Normal flow:

1. Pull / rebuild the new version.
2. Replace the JAR.
3. Restart the service.

On restart:
- Migrator runs any new migrations listed in `migrations/manifest.txt`.
- Config validator checks `targets.json` — if the upgrade introduced a new
  mandatory field, startup will fail loudly.
- `resetStaleRunning` clears any mid-flight state from the previous process.

For breaking schema changes, the changelog / release notes will call them out.
Per-target DDL changes (`STG_*` / `T_*` column shape) are your responsibility —
adjust them before or alongside the code upgrade.
