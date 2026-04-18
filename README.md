# ofload

> **REST-triggered incremental ETL for Oracle Fusion — drop-in replacement for BI Publisher adapter calls in OIC, with a built-in UI for good measure.**

`ofload` is a small HTTP service that copies data from **Oracle Fusion Cloud** into
**Oracle Autonomous Database** on demand. Trigger a run with a single `POST /run`
from **OIC, a cron job, Jenkins, Airflow, or anything else that speaks HTTP** —
the service does streaming extraction, tuple-seek pagination, MERGE idempotency,
watermark tracking, and per-target row-count bookkeeping. It's also the canonical
reference implementation for [**ofjdbc**](https://github.com/sergey-rudenko-ba/ofjdbc),
the JDBC driver that turns any Fusion tenant into a queryable SQL source.

The built-in dashboard exists for humans (ops, dev, demos). In production,
**scheduling and orchestration live in your existing tooling** — `ofload` is
just the worker on the other end of the HTTP call.

---

## The 30-second pitch

Adding a new Fusion table to your warehouse looks like this:

```json
{
  "name": "gl_je_headers",
  "stagingTable": "STG_GL_JE_HEADERS",
  "targetTable":  "T_GL_JE_HEADERS",
  "watermarkColumn": "LAST_UPDATE_DATE",
  "watermarkType":   "TIMESTAMP",
  "keyColumns": ["JE_HEADER_ID"],
  "sourceQuery": "SELECT JE_HEADER_ID, LAST_UPDATE_DATE, ... FROM GL_JE_HEADERS WHERE /*WM*/",
  "targetInsert": "INSERT INTO STG_GL_JE_HEADERS (...) VALUES (?, ?, ...)",
  "targetMerge":  "MERGE INTO T_GL_JE_HEADERS tgt USING STG_GL_JE_HEADERS src ON (...) ...",
  "initialWm": "1970-01-01 00:00:00",
  "pageSize": 10000,
  "lookbackMinutes": 60,
  "group": 1
}
```

That's the entire integration. No BI Publisher data model. No visual flow. No
`.iar` export. The pipeline handles pagination, watermark tracking, retries,
parallel execution, and MERGE idempotency automatically.

---

## Why it exists

Oracle Fusion does not expose direct SQL access. The "official" options to get
data out are:

- **OIC** — heavyweight iPaaS, per-message licensing, visual flows that resist
  version control, timeout issues on large extracts.
- **BICC** — only works for predefined View Objects, needs Object Storage and
  a separate loader, no access to custom attributes.
- **BI Publisher reports** — one report per table, manual XML → row parsing.

None of these is great for "continuously copy N Fusion tables into a data
warehouse". `ofjdbc` solves the access problem with a single universal report
(`RP_ARB.xdo`) and a JDBC driver on top. `ofload` solves the pipeline problem.

See [docs/why-ofjdbc.md](docs/why-ofjdbc.md) for the detailed comparison.

---

## Quick start

```bash
git clone https://github.com/<you>/ofload.git
cd ofload

# Set up credentials (see docs/env-vars.md for the full list).
export DB_USER=... DB_PASSWORD=... DB_CONNECT_STRING=...
export TNS_ADMIN=/path/to/wallet
export SOURCE_URL=... SOURCE_DRIVER_CLASS=my.jdbc.wsdl_driver.WsdlDriver

# Build + run (applies schema migrations, starts HTTP server on :8080).
./gradlew run
```

Trigger a run via REST (the primary production interface):

```bash
# Run everything
curl -X POST http://localhost:8080/run

# Run a specific group (dimensions first, facts after — see targets-json.md)
curl -X POST http://localhost:8080/run/group/1

# Run a single target
curl -X POST http://localhost:8080/run/target/gl_je_headers

# Poll status
curl http://localhost:8080/status | jq
```

Or open `http://localhost:8080/` and hit **Run All Groups** in the dashboard
for interactive use.

### Calling from OIC

The most common production pattern: an OIC scheduled integration triggers
`ofload` via REST, OIC retains scheduling/alerting/audit, `ofload` does the SQL.

1. Create a **REST connection** (Invoke role) pointing at `http://<ofload-host>:8080`.
2. Create a **Scheduled Orchestration** with one Invoke action: `POST /run/group/1` (no body).
3. Configure the response with this sample to let OIC generate the schema:
   ```json
   { "runId": "abc-123-def", "targets": ["gl_je_headers"], "skipped": [], "message": "Group 1 started" }
   ```
4. **Fire-and-forget** — end the integration immediately after the 202 response. OIC has a hard 5-minute execution timeout; ETL runs are longer. `ofload` keeps running in the background after HTTP closes.
5. Schedule the integration and activate.

Full walkthrough (fault handling, status codes, what to do instead of
polling) lives in [docs/operations.md](docs/operations.md#oic-integration-example).

Full service walkthrough: [docs/getting-started.md](docs/getting-started.md).

---

## Features

- **REST-first control plane** — `POST /run`, `/run/group/{id}`, `/run/target/{name}`
  return 202 + a `runId`; poll `/status` for completion. Designed to be called
  from OIC, cron, k8s Jobs, or any scheduler you already have.
- **Tuple-seek pagination** — correctly resumes across watermark ties, constant
  memory regardless of table size.
- **One SSO per process** — driver's token cache is race-guarded so parallel
  workers don't each open a browser.
- **MERGE idempotency** — re-running is always safe; no tracker tables, no
  two-phase commits.
- **Split run state** — ETL_WATERMARK cleanly separates "current run",
  "last success", and "last failure" — dashboard never shows stale numbers.
- **Prometheus `/metrics`** — runs, row throughput, page durations, retry
  counts, pool pressure (see [docs/operations.md](docs/operations.md)).
- **Fail-fast config validation** — `targets.json` is checked on startup;
  missing `/*WM*/` markers, column-count mismatches, and invalid timestamps
  refuse to start the service.
- **Schema migrations on startup** — infrastructure DDL applied automatically
  via a tiny homegrown migrator (no Flyway dependency).
- **Dashboard + read-only targets page** — browse live run state and the loaded
  config; nice for humans, not required for operation.
- **Cheap to run** — single 40 MB JAR, constant ~256 MB heap regardless of
  table size. Runs on OCI Always Free tier, a $5 droplet, or a 250m-CPU k8s
  pod. No per-message licensing. See
  [docs/operations.md](docs/operations.md#where-this-runs-and-why-its-cheap).

---

## What's inside

```
src/main/kotlin/app/
  Main.kt                  entry point
  config/                  targets.json + validation
  db/                      Oracle ATP (UCP pool) + Fusion (ofjdbc) wiring
  etl/                     Pipeline, StagingLoader, TargetMerger, WatermarkStore
  migrations/              startup schema migrator
  metrics/                 Micrometer / Prometheus registry
  server/                  Javalin HTTP, RunManager, row-count cache
src/main/resources/
  targets.json             pipeline definition
  migrations/              SCHEMA_MIGRATIONS-tracked SQL files
  static/                  dashboard + targets viewer
docs/                      all the deep-dive docs linked above
```

75 unit + 8 integration tests. Total Kotlin LOC under ~2 500.

## HTTP surface at a glance

```
POST /run                     → kick off all targets                 → 202 {runId, ...}
POST /run/group/{id}          → kick off one group                   → 202 {runId, ...}
POST /run/target/{name}       → kick off one target                  → 202 {runId, ...}
GET  /status                  → per-target live state (JSON)         → 200 {targets: [...]}
GET  /metrics                 → Prometheus scrape                    → 200 text/plain
GET  /live  /ready  /health   → k8s probes                           → 200/503
GET  /                        → dashboard (HTML, optional)           → 200
```

All POSTs are non-blocking — runs happen on a bounded thread pool, status is
observed via polling. 409 on already-running, 503 on queue overflow, 202 on
accepted. Perfect fit for OIC HTTP adapter, cron+curl, k8s Jobs, or any other
scheduler you already have.

---

## Status

Production-grade on the feature axis (streaming, retries, metrics, graceful
shutdown). The test story is solid on unit coverage; full end-to-end
integration with a live Fusion is exercised by the author but not yet
published as a CI-friendly suite.

## License

TBD — see `LICENSE`.

## Credits

Built on top of **[ofjdbc](https://github.com/sergey-rudenko-ba/ofjdbc)**.
If you find `ofload` useful, star that repo too.
