# Architecture

This document walks through the full pipeline end-to-end and explains the design
decisions that shape it. Aimed at people who want to understand *why* the code
is structured the way it is before changing it.

## System diagram

```
                       ┌───────────────────────────────────────────────┐
                       │                  ofload (JVM)                 │
                       │                                               │
  Oracle Fusion        │  ┌───────────┐  ┌───────────┐  ┌───────────┐  │     Oracle ATP
  ┌─────────────┐      │  │           │  │           │  │           │  │     ┌────────────┐
  │             │ WSDL │  │  Source   │  │ Staging   │  │  Target   │  │ JDBC│            │
  │ BI Publisher│◄─────┼──┤  Loader   ├──▶ Loader    ├──▶  Merger   ├──┼────▶│  ATP +     │
  │ RP_ARB.xdo  │─────▶│  │           │  │           │  │           │  │     │  UCP pool  │
  │             │ rows │  └─────▲─────┘  └───────────┘  └───────────┘  │     │            │
  └─────────────┘      │        │              │              │        │     └────────────┘
                       │   ofjdbc driver       │              │        │       ▲    ▲   ▲
                       │   (BROWSER / BASIC)   ▼              ▼        │       │    │   │
                       │                  Watermark &  ETL_WATERMARK ──┼───────┘    │   │
                       │                  run-state tracking           │            │   │
                       │                       │              ▲        │            │   │
                       │                       ▼              │        │            │   │
                       │  ┌────────────────────────────────┐  │        │            │   │
                       │  │  HTTP surface (Javalin)        │  │        │  STG_* ────┘   │
                       │  │   /status   /run   /metrics    │  │        │            ────┘
                       │  │   /live     /ready /health     │  │        │  T_*
                       │  └────────────────────────────────┘  │        │
                       │                                      │        │
                       │  Startup: migrations → config        │        │
                       │           validation → stale-run     │        │
                       │           cleanup → start server     │        │
                       └──────────────────────────────────────┘        └────────────
```

## Data flow per page

```
1. beginRun          ─ UPDATE ETL_WATERMARK   SET CURRENT_RUN_STARTED = now, CURRENT_RUN_ID = uuid
                       (clears CURRENT_RUN_ROWS, leaves LAST_SUCCESS_*/LAST_FAILURE_* intact)

2. TRUNCATE STG_*    ─ DDL (implicit commit). Staging holds only the current page.

3. Apply NLS         ─ ALTER SESSION SET NLS_... (per-target overrides)

4. Source SELECT     ─ sourceQuery with /*WM*/ replaced:
                       • first page  : watermark > ?
                       • seek page   : (wm > ? OR (wm = ? AND keys > seek_key))
                       • ORDER BY watermark, keys  (when pageSize is set)
                       • driver adds OFFSET/FETCH automatically

5. Stream rows       ─ rs.next() → insPs.setObject() → insPs.addBatch()
                       every BATCH_SIZE rows: executeBatch() + commit()
                       Java-side break at pageSize (source compliance not trusted)

6. MAX(wm)           ─ SELECT MAX(watermarkColumn) FROM STG_*   → new_wm

7. MERGE             ─ targetMerge executed verbatim

8. updateProgress    ─ UPDATE ETL_WATERMARK SET LAST_WM_* = new_wm, CURRENT_RUN_ROWS += N

9. Loop or finish    ─ page full → goto 2 with seek_key = last row's keys
                       short page → finishRunOk (clear CURRENT_RUN_*, set LAST_SUCCESS_*)

On any throw:
   rollback() the current connection
   failRun() on a fresh connection (the main conn may be poisoned)
   log the error + mark FAILED in LAST_FAILURE_*
   re-throw if it's an Error (OOM) so the JVM can terminate cleanly
```

## Key design decisions

### 1. Streaming, not buffering

The loader reads source rows directly into the staging `PreparedStatement` batch
— no intermediate `List<Row>` ever exists. Memory is constant (`BATCH_SIZE`
rows worth of `setObject` state), regardless of page size or total table size.

This was not always the case — early versions buffered into a `MutableList` for
readability. When production tables hit 500 k-row single-page extracts, the JVM
ran out of heap. Streaming is the current design.

### 2. Tuple-seek pagination

The naive `WHERE watermark > :last_wm` page boundary loses rows that share the
same watermark value as the last row of the previous page. Production Fusion
data frequently has hundreds or thousands of rows sharing a single
`LAST_UPDATE_DATE` (end-of-period batch jobs, subledger auto-posts).

Tuple-seek adds a tiebreaker:

```sql
WHERE (wm > :last_wm OR (wm = :last_wm AND (k1, k2, …) > (:last_k1, :last_k2, …)))
ORDER BY wm, k1, k2, …
```

Row-value comparison in Oracle is strictly lexicographic, matching the
`ORDER BY`. The cursor advances monotonically even across watermark ties.

For single-column keys the generated predicate uses scalar form
(`k > :last_k`) instead of `(k) > (:last_k)`. The two are semantically
equivalent in Oracle, but the BI Publisher RP_ARB.xdo parser has been observed
to mishandle the row-value form for single-column cases — so single keys get
scalar, multi keys get tuple.

### 3. One staging table per target

Every target has its own dedicated `STG_*` table rather than a shared
staging area. This:

- Allows parallel runs without cross-target locking.
- Makes restarts safe — TRUNCATE `STG_<name>` at the start of each page
  never races with another target.
- Simplifies MERGE — `USING STG_<name>` is unambiguous.

Cost: one DDL file per target. Acceptable for a repo where integrations are
version-controlled anyway.

### 4. MERGE idempotency as the correctness backbone

Every downstream commit of source data is a MERGE into the final target.
Rerunning the same data just updates the same rows. This eliminates the need
for:

- Tracker tables of "which rows have already been processed".
- Distributed locks to prevent concurrent duplicate inserts.
- Two-phase commit between data load and progress marker.

If a run fails mid-way, the next run starts from the last committed watermark
and MERGE handles the overlap. The worst case is a few thousand rows of
repeated work — never a duplicate in the target.

### 5. Split run-state in ETL_WATERMARK

An earlier schema stored a single `LAST_STATUS` / `LAST_RUN_FINISHED` / `ROWS_LOADED`
triple. On the dashboard this produced misleading UIs: "RUNNING for 20 minutes,
12 000 rows loaded, finished 3 hours ago". The UI couldn't distinguish the
previous run's artifacts from the current run's state.

The current schema splits them:

| Section | Columns | When populated |
|---|---|---|
| Progress cursor | `LAST_WM_TIMESTAMP` / `LAST_WM_NUMBER` | Advances per page within a run |
| Current run | `CURRENT_RUN_STARTED`, `CURRENT_RUN_ID`, `CURRENT_RUN_ROWS` | Set by `beginRun`, cleared by `finishRunOk` / `failRun` |
| Last success | `LAST_SUCCESS_FINISHED`, `LAST_SUCCESS_ROWS`, `LAST_SUCCESS_DURATION_MS` | Set only by `finishRunOk` |
| Last failure | `LAST_FAILURE_FINISHED`, `LAST_FAILURE_ERROR` | Set only by `failRun` |

The dashboard renders the status as:

```
currentRunStarted ≠ null             → RUNNING
else max(lastSuccess, lastFailure)   → OK / FAILED, whichever is newer
else                                 → NEW
```

Rows shown: `currentRunRows` during RUNNING, `lastSuccessRows` when idle.
Duration: live elapsed from `currentRunStarted` during RUNNING, stored
`lastSuccessDurationMs` when idle. No stale visuals.

### 6. Typed watermark columns

`LAST_WM_TIMESTAMP TIMESTAMP(6)` and `LAST_WM_NUMBER NUMBER` store the typed
value based on `watermarkType`. The legacy `LAST_WM VARCHAR2(64)` column
stays physically in the schema (from migration 001) but no code reads or
writes it; migration 003 can drop it once confidence is high.

Typing catches format errors at write time (Oracle rejects bad timestamps) and
gives predictable ordering on the DB side without string-lexicographic
surprises.

Binding into the source SELECT still happens as a string — `setString` is what
ofjdbc accepts, and the canonical format
`yyyy-MM-dd HH:mm:ss.SSSSSS` round-trips through Oracle's TIMESTAMP(6) without
loss.

### 7. One SSO per process (race-guarded)

ofjdbc owns a process-wide `TokenCache` keyed by host. First connection opens
a browser (for BROWSER auth); subsequent connections hit the cache and return
immediately.

The catch: `BrowserAuthenticator.authenticate()` has an unguarded
`check cache → launch browser → write cache` sequence. Three parallel workers
starting simultaneously all miss the cache and all open browsers.

`SourceDs` guards exactly that critical section with a `ReentrantLock` around
`DriverManager.getConnection()`:

```kotlin
openLock.lock()
try { DriverManager.getConnection(url, props) }
finally { openLock.unlock() }
```

First caller pays SSO cost. Second caller waits briefly on the lock, finds a
populated cache, returns immediately. No mutex is held during actual query
execution — parallel reads happen fully concurrently afterward.

### 8. Fail-fast config validation

Startup runs `TargetsValidator.validate(config)` before the HTTP server comes
up. Missing `/*WM*/`, column-count mismatches, invalid watermarkType, bad
`initialWm` format — all reported at once, all cause the service to refuse
to start. This catches 90% of the "only broke on first row of first run"
class of bugs as an operator error at deploy time.

### 9. Homegrown migrations runner

Infrastructure DDL (ETL_WATERMARK, SCHEMA_MIGRATIONS, typed watermark
columns) is applied automatically on startup via a ~150-line migrator. The
manifest lists migration files in order; each one is a single SQL statement
(typically a PL/SQL block with `EXCEPTION WHEN OTHERS` to swallow
"already exists" errors on upgrades).

Flyway would bring ~5 MB of dependency. For one tracking table and a handful
of schema-evolution scripts, the homegrown approach is right-sized.

Per-target DDL (`STG_*`, `T_*`) stays manual by design — the per-target
schemas depend on application needs and shouldn't auto-apply.

### 10. Bounded pools + backpressure

Two thread pools, both bounded:

- `RunManager.executor` — accepts HTTP `/run*` submissions. `RUNNER_THREADS` × `RUNNER_QUEUE_SIZE` (default 4 × 16). Overflow → `AbortPolicy` → 503 "Runner queue full". Operator sees an honest error instead of an invisible queue.
- `Pipeline.etlPool` — runs parallel target workers. `ETL_WORKERS` × `ETL_QUEUE_SIZE` (default 8 × 32). Overflow → `CallerRunsPolicy` — the outer runner thread inlines the task. Serialises excess instead of rejecting, so a `/run` doesn't mid-group fail just because of internal backpressure.

### 11. Thread-local `MDC` for structured logs

Every log line emitted inside a run carries MDC fields:

- `target` — which target is logging.
- `runId` — UUID issued by RunManager, correlates with the POST /run response.
- `page` — current page number within the run.
- `group` — group id during parallel-group execution.

This lets `ofload.target = gl_je_headers AND ofload.runId = abc-123` queries in
any log aggregator (OCI Logging, Loki, ELK) pull a single run out of mixed
concurrent output without grepping.

## Module layout

```
app/
├── Main.kt                    entry point: load config → validate → migrate → reset stale → start HTTP
├── config/
│   ├── PipelineConfig.kt      @Serializable data classes + JSON loader
│   └── TargetsValidator.kt    static validation (missing markers, column mismatches, types)
├── db/
│   ├── OracleDs.kt            UCP pool + NLS baseline on each borrow
│   └── SourceDs.kt            ofjdbc wrapper with auth-lock + per-call connection
├── etl/
│   ├── Pipeline.kt            orchestrator: groups, parallelism, per-target lifecycle
│   ├── StagingLoader.kt       query builder (pure) + streaming load + Java-side pageSize cap
│   ├── TargetMerger.kt        executes targetMerge verbatim, records metrics
│   └── WatermarkStore.kt      CRUD on ETL_WATERMARK, typed watermark columns
├── migrations/
│   └── Migrator.kt            reads manifest.txt + runs pending SQL in order
├── metrics/
│   └── Metrics.kt             Micrometer / Prometheus registry
└── server/
    ├── Main HTTP endpoints    Routes.kt, TargetsApi.kt
    ├── Model.kt               DTOs (WatermarkRow, StatusResponse, etc.)
    ├── RunManager.kt          /run submission pool, running-set lock, graceful shutdown
    └── TargetRowCountCache.kt TTL cache for COUNT(*) shown on /status
```

## HTTP surface

The HTTP surface **is** the control plane. There is no CLI, no message queue,
no internal scheduler — everything external (OIC, cron, k8s CronJob, Airflow)
talks to `ofload` through this API.

**Primary endpoints — run triggering:**

| Method | Path | Purpose |
|---|---|---|
| POST | `/run` | Run every target in the config |
| POST | `/run/group/{id}` | Run all targets in a group (parallel within group, sequential barrier between groups) |
| POST | `/run/target/{name}` | Run one target |
| GET | `/status` | Full per-target state — poll this to observe progress and correlate with the `runId` from POST /run |

All POST endpoints return 202 immediately and run asynchronously on
`RunManager.executor` (HTTP thread pool). Duplicates for currently-running
targets return 409. Backpressure from the bounded queue returns 503.

**Secondary endpoints — health and observability:**

| Method | Path | Purpose |
|---|---|---|
| GET | `/live` | Liveness (no DB) — for k8s livenessProbe |
| GET | `/ready` | Readiness (ATP ping + source driver loaded) — for readinessProbe / LB |
| GET | `/health` | Legacy alias of /ready |
| GET | `/metrics` | Prometheus scrape endpoint |

**Tertiary endpoints — UI and config introspection:**

| Method | Path | Purpose |
|---|---|---|
| GET | `/` | Dashboard HTML |
| GET | `/targets` | Read-only config viewer HTML |
| GET | `/config` | Thin target list for dashboard navigation |
| GET | `/api/targets` | JSON dump of loaded config |
| GET | `/api/targets/{name}` | Single target config |
| GET | `/api/targets/{name}/ddl` | Reverse-engineered DDL from DB |

The UI endpoints (`/`, `/targets`, `/api/*`) exist for humans. In a headless
production deployment they can be firewalled off without losing any
functionality — OIC/cron only need `/run*` and `/status`.

## Further reading

- [targets-json.md](targets-json.md) — full config field reference.
- [env-vars.md](env-vars.md) — every tuning knob, including ofjdbc's.
- [operations.md](operations.md) — deployment, observability, troubleshooting.
- [why-ofjdbc.md](why-ofjdbc.md) — the positioning vs OIC, BICC, BIP reports.
- [proposals/](proposals/) — upstream feature requests for the ofjdbc driver.
