# Getting started

Zero to a working incremental pipeline in 10–15 minutes. This walkthrough assumes
you have access to an Oracle Fusion instance and an Oracle Autonomous Database
(ATP or ADW) tenant.

## Prerequisites

- **JDK 21+** (project builds with Kotlin 2.2, targets JVM 21).
- **Oracle ATP / ADW** instance with:
  - A database user that can `CREATE TABLE` (for `ETL_WATERMARK`,
    `SCHEMA_MIGRATIONS`, and your `STG_*` / `T_*` tables).
  - A downloaded wallet (`.zip` → unzipped directory) if you're using mTLS
    (recommended for ATP).
- **Oracle Fusion access** with an account able to run BI Publisher reports
  (the exact privilege is `BI Administrator` or `BI Publisher Developer`).
- **ofjdbc driver jar** — NOT shipped in this repo (the ~80 MB shaded JAR
  would bloat git). Download the latest release from
  [github.com/krokozyab/ofjdbc/releases](https://github.com/krokozyab/ofjdbc/releases),
  save it as `libs/orfujdbc-1.0-SNAPSHOT.jar` (exact filename matters —
  that's what `build.gradle.kts` references), then continue with the build
  step below.

## 1. Clone the repo and install the ofjdbc driver

```bash
git clone https://github.com/<you>/ofload.git
cd ofload

# Download the ofjdbc driver JAR from its GitHub releases page
# https://github.com/krokozyab/ofjdbc/releases
# Then drop it into libs/ under the exact filename the build expects:
mkdir -p libs
curl -L -o libs/orfujdbc-1.0-SNAPSHOT.jar \
  https://github.com/krokozyab/ofjdbc/releases/download/<release-tag>/orfujdbc-1.0-SNAPSHOT.jar

# Build + unit tests (integration tests skip without DB credentials)
./gradlew build
```

This produces `build/libs/ofload-1.0-SNAPSHOT-all.jar`. If the build fails
with `orfujdbc-1.0-SNAPSHOT.jar not found`, the driver isn't in `libs/` —
go back and download it.

## 2. Configure environment

Create a `.env` file or export these directly. Minimum set:

```bash
# Target: Oracle ATP
export DB_USER="ETL_OWNER"
export DB_PASSWORD="<ATP password>"
export DB_CONNECT_STRING="yourdb_medium"            # TNS alias from tnsnames.ora
export TNS_ADMIN="/absolute/path/to/wallet/folder"  # directory with the unzipped wallet files

# Source: Oracle Fusion via ofjdbc
export SOURCE_DRIVER_CLASS="my.jdbc.wsdl_driver.WsdlDriver"
export SOURCE_URL="jdbc:wsdl://your-fusion-host.oraclecloud.com"

# Authentication mode — pick one:
#
# (a) BROWSER SSO — interactive, opens a browser on first use, caches the token
#     (~55 min). Great for dev/demo, NOT suitable for unattended cron.
export SOURCE_AUTH_TYPE="BROWSER"
#
# (b) BASIC auth — static user+password, works in headless environments.
#     Use a dedicated Fusion service account in production.
# export SOURCE_AUTH_TYPE="BASIC"
# export SOURCE_USER="your.fusion.user"
# export SOURCE_PASSWORD="…"
#
# (c) BEARER token — raw JWT in SOURCE_PASSWORD. Tokens expire in ~1 h with
#     no auto-refresh; mostly useful for short-lived test harnesses.
# export SOURCE_AUTH_TYPE="BEARER"
# export SOURCE_PASSWORD="eyJhbGciOiJSUzI1Ni…"
```

See [operations.md — Authentication modes](operations.md#authentication-modes)
for the full decision matrix and production recommendations.

The full env-var reference (including tuning knobs for pools, timeouts,
prefetch, metrics TTLs) lives in [env-vars.md](env-vars.md).

## 3. Prepare the schema

The service applies infrastructure DDL automatically on startup via its
built-in migrator. You do **not** need to run any setup SQL manually for
`ETL_WATERMARK` or `SCHEMA_MIGRATIONS`.

You **do** need to create each target's `STG_*` and `T_*` tables by hand,
because their schemas are application-specific. Example DDL lives in
`src/main/resources/ddl/`:

```bash
# Pick the staging + target DDL that matches your first target and run them
# in SQL*Plus / SQLcl / SQL Developer connected to your ATP schema.
src/main/resources/ddl/11_stg_gl_je_headers.sql
src/main/resources/ddl/21_t_gl_je_headers.sql
```

Or roll your own — the only contract is that the column list in your
`targetInsert` matches the staging table's columns.

## 4. Point `targets.json` at a real table

`src/main/resources/targets.json` ships with three example targets. To start
with a single one, trim the file to just one entry. A minimal valid target:

```json
{
  "targets": [
    {
      "name": "gl_je_headers",
      "stagingTable": "STG_GL_JE_HEADERS",
      "targetTable":  "T_GL_JE_HEADERS",
      "watermarkColumn": "LAST_UPDATE_DATE",
      "watermarkType":   "TIMESTAMP",
      "keyColumns": ["JE_HEADER_ID"],
      "sourceQuery":   "SELECT JE_HEADER_ID, LAST_UPDATE_DATE, LEDGER_ID, NAME FROM GL_JE_HEADERS WHERE /*WM*/",
      "targetInsert":  "INSERT INTO STG_GL_JE_HEADERS (JE_HEADER_ID, LAST_UPDATE_DATE, LEDGER_ID, NAME) VALUES (?, ?, ?, ?)",
      "targetMerge":   "MERGE INTO T_GL_JE_HEADERS tgt USING STG_GL_JE_HEADERS src ON (tgt.JE_HEADER_ID = src.JE_HEADER_ID) WHEN MATCHED THEN UPDATE SET tgt.LAST_UPDATE_DATE = src.LAST_UPDATE_DATE, tgt.LEDGER_ID = src.LEDGER_ID, tgt.NAME = src.NAME WHEN NOT MATCHED THEN INSERT (JE_HEADER_ID, LAST_UPDATE_DATE, LEDGER_ID, NAME) VALUES (src.JE_HEADER_ID, src.LAST_UPDATE_DATE, src.LEDGER_ID, src.NAME)",
      "initialWm": "1970-01-01 00:00:00",
      "pageSize":  10000
    }
  ]
}
```

Key things to get right on the first try:

- **`/*WM*/` marker** in `sourceQuery` is **required** — the pipeline refuses
  to start without it.
- **Column count must match** between `sourceQuery` SELECT list and
  `targetInsert` placeholder list. The startup validator catches mismatches.
- **`keyColumns` must appear in the SELECT list** and be used in `targetMerge`'s
  `ON` clause. The validator checks the first but not the second.

See [targets-json.md](targets-json.md) for every field in detail.

## 5. Run the service

```bash
./gradlew run
```

First startup does three things you'll see in the log:

```
Loaded 1 target(s): [gl_je_headers]
targets.json validation passed
Bootstrapping tracking table SCHEMA_MIGRATIONS
Applying 2 pending migration(s): [001_init_etl_watermark.sql, 002_typed_watermark_columns.sql]
Schema migrations complete.
Service started on port 8080
```

Open `http://localhost:8080/`. You'll see the dashboard with your target in
`NEW` status.

## 6. Trigger the first run (REST — the primary interface)

Runs are kicked off via HTTP. This is how you'll trigger them in production
from OIC, cron, a k8s CronJob, or any scheduler. The dashboard button is a
convenience wrapper around the same endpoints.

```bash
# Run a single target
curl -X POST http://localhost:8080/run/target/gl_je_headers
# → 202 Accepted
# {"runId":"abc-123-def","targets":["gl_je_headers"],"skipped":[],"message":"Target gl_je_headers started"}

# Or run a whole group
curl -X POST http://localhost:8080/run/group/1

# Or run every target in the config
curl -X POST http://localhost:8080/run
```

First time through, the ofjdbc driver will open a browser for SSO if you're
using browser-based auth. Approve the login; the token gets cached for
~55 minutes. For unattended production runs see the auth section in
[operations.md](operations.md).

Poll `/status` to watch progress:

```bash
curl -s http://localhost:8080/status | jq '.targets[] | {name, running, watermark: {currentRunStarted, currentRunRows, lastWm, lastSuccessFinished}}'
```

Or open the dashboard at `http://localhost:8080/` for the same data in a UI.
Either way you'll see:

- Status flips to `RUNNING`.
- `currentRunRows` increments live (refreshed every page commit).
- `lastWm` advances as pages complete.
- When the source returns a short (< pageSize) page, the page-loop exits,
  status becomes `OK`, `lastSuccessDurationMs` captures the elapsed time.

## 7. Verify the data

```sql
SELECT TARGET_NAME, LAST_SUCCESS_FINISHED, LAST_SUCCESS_ROWS,
       LAST_WM_TIMESTAMP, LAST_SUCCESS_DURATION_MS
  FROM ETL_WATERMARK;

SELECT COUNT(*) FROM T_GL_JE_HEADERS;
```

Run the target again — it should complete in a few seconds (nothing new to
load since the last `LAST_UPDATE_DATE`).

## 8. Wire it into your scheduler

`ofload` has no internal scheduler on purpose — whatever cadence you want is
controlled from the outside. Common patterns:

**Cron on the host**
```
*/15 * * * * curl -fsS -X POST http://ofload:8080/run > /var/log/ofload-trigger.log
```

**Kubernetes CronJob**
```yaml
apiVersion: batch/v1
kind: CronJob
spec:
  schedule: "*/15 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: trigger
              image: curlimages/curl
              command: ["curl","-fsS","-X","POST","http://ofload:8080/run"]
```

**OIC scheduled integration**
Create a scheduled flow whose single action is an HTTP POST to
`http://ofload:8080/run/group/{id}`. OIC retains its scheduling / fault
handling / notification; `ofload` does the actual SQL work.

**Airflow, Jenkins, OCI Scheduler** — same pattern, one HTTP POST.

For orchestrators that need to wait for completion, poll `/status` until the
target returns to non-RUNNING state. The response body includes `runId` so
you can correlate logs.

## 9. Next steps

- Add more targets — see [targets-json.md](targets-json.md) for field details.
- Wire up Prometheus scraping at `/metrics` — see [operations.md](operations.md).
- Review [architecture.md](architecture.md) for the design decisions behind
  streaming, pagination, and run-state modelling.
- For production unattended use (cron, k8s), read the auth section in
  [operations.md](operations.md) — browser SSO is fine for dev/demo but not
  for cron.

## Troubleshooting first-run issues

| Symptom | Likely cause | Fix |
|---|---|---|
| `targets.json validation failed` | `/*WM*/` missing or column mismatch | Read the listed errors; they cite the exact field |
| `ORA-00942: table or view does not exist` during merge | `STG_*`/`T_*` not created | Apply the per-target DDL from `src/main/resources/ddl/` |
| Browser doesn't open | `SOURCE_URL` mistake, or running headless | Verify `SOURCE_URL`; for headless use BASIC auth |
| `Source returned more than pageSize rows` log | BI Publisher ignoring limits | Should not happen with current code — raise an issue |
| Runs hang on "ATP connected" | Wallet path wrong or firewall | Check `TNS_ADMIN` points to unzipped wallet, not the zip |

If a run fails, the error lives in `ETL_WATERMARK.LAST_FAILURE_ERROR` and in
the service log (structured MDC includes `target`, `runId`, `page`). Fix the
cause, hit Retry — MERGE idempotency means re-runs are always safe.
