# Why ofjdbc

Oracle Fusion Cloud does not expose direct SQL access to its database tier.
Any "copy Fusion data into a warehouse" problem becomes a question of **which
indirect access layer to use**. This document surveys the options and explains
why `ofjdbc` — plus a small pipeline around it — beats the alternatives for
code-first ETL.

## The four contenders

| Approach | Access method | What it gives you |
|---|---|---|
| **Oracle Integration Cloud (OIC)** | Visual flow → BI Publisher report → DB adapter | Managed iPaaS, per-message licensing, XML flows |
| **BI Cloud Connector (BICC)** | Predefined View Objects → CSV in Object Storage | Official bulk extract, limited to catalogued VOs |
| **Hand-written BI Publisher reports** | SOAP call per report, custom XML parser | Full control but one report per table |
| **ofjdbc (this project's foundation)** | Standard JDBC → universal BI Publisher report | SQL against any Fusion table through one driver |

## The universality argument

OIC and BICC share a structural cost: **one artifact per table you want**.

```
OIC per table:
  1. BI Publisher Data Model (SQL + parameters)
  2. BI Publisher Report (wraps the data model)
  3. OIC Integration flow (visual, one per integration)
  4. Tracker / lookup configuration (overlap windows, status)

BICC per table:
  1. Pick the View Object from the catalog (if it exists)
  2. Configure incremental extraction
  3. Downstream file-to-DB loader (ODI / OCI DI / custom)
  4. Object Storage lifecycle policy for extracted files
```

Scaling to 10 or 50 tables means multiplying all of these. Each carries its
own error handling, its own field mappings, its own deployment story.

**`ofjdbc` replaces this with one driver and one report.** Any SELECT against
any Fusion table goes through the same `/Custom/CloudSQL/RP_ARB.xdo` universal
report. Adding a new table is:

```json
{
  "sourceQuery": "SELECT ... FROM PO_HEADERS_ALL WHERE /*WM*/",
  "pageSize": 10000
}
```

No new report. No new flow. No new CSV lifecycle.

| Tables to export | OIC artifacts | BICC artifacts | `ofload` artifacts |
|---|---|---|---|
| 1 | 1 report + 1 flow + 1 tracker | 1 VO + 1 load config + 1 bucket rule | 1 JSON entry |
| 10 | 10 + 10 + 10 | 10 + 10 + 10 | 10 JSON entries |
| 50 | 50 + 50 + 50 | 50 + 50 + 50 | 50 JSON entries |

## Where the other options fail

### OIC

OIC is the right tool for **event-driven integration between heterogeneous
systems** (Fusion ↔ Salesforce, Fusion ↔ SAP, webhook triggers, EDI). For the
specific job of "batch-copy Fusion tables into a warehouse" it underperforms:

- **Per-message licensing** scales linearly with data volume. A daily AR/AP
  export hitting millions of rows burns message packs fast.
- **Visual XML flow definitions** resist code review and diffing. Rolling back
  a broken integration means re-importing a previous `.iar` binary.
- **Fault handling is bolted on.** Custom tracker tables or status flags are
  needed to prevent duplicates — data load and tracker update are separate
  operations, so transient failures create half-committed state.
- **Timeouts on large extracts** are an operational fact. Bigger tables
  require manual date-window segmentation that lives in an OIC configuration
  screen, not in version control.

### BICC

BICC is closer in purpose to `ofload` — it's designed specifically for
warehouse loads. It fails on **scope and architecture**:

- **VO catalog gaps.** Subledger Accounting (`XLA_AE_HEADERS`,
  `XLA_DISTRIBUTION_LINKS`), Fixed Assets detail (`FA_ADJUSTMENTS`,
  `FA_DEPRN_DETAIL`), and anything created via Application Composer often have
  no BICC VO. You discover this when you need the data and hit a wall.
- **No join control.** Each VO extracts independently. Cross-module queries
  (GL → XLA → FA → custom attributes) are impossible — you must extract each
  piece and re-join downstream, by hand.
- **Three-hop architecture.** `Fusion → Object Storage → Load tool → Target
  DB` has real operational weight: bucket lifecycle, IAM policies, partial-file
  cleanup, double monitoring surface.
- **No custom attributes.** DFF/EFF flexfields stored in `ATTRIBUTE_*` columns
  are invisible to BICC unless the VO explicitly surfaces them.

### Hand-written BI Publisher reports

This is the dark-matter option — most teams eventually end up building
half of it when OIC or BICC doesn't fit. Per-table BIP reports with custom XML
parsers give you full SQL control but:

- Every table is a new report.
- Every schema change is a report edit.
- XML parsing is on you.
- Authentication, session reuse, pagination — all on you.

At this point you've built a worse version of `ofjdbc`.

## What `ofjdbc` actually does

`ofjdbc` is a JDBC driver that:

1. Receives standard SQL via `DriverManager.getConnection(url).prepareStatement(sql)`.
2. Wraps it into a SOAP request to a single universal BI Publisher report
   (`/Custom/CloudSQL/RP_ARB.xdo`).
3. Parses the XML response into a streaming `ResultSet`.
4. Implements its own OFFSET/FETCH pagination underneath — any SELECT that
   doesn't contain ROWNUM/FETCH/OFFSET gets auto-paginated.
5. Handles SSO authentication (BROWSER / BASIC / BEARER) with a process-wide
   token cache.
6. Retries transient transport failures (HTTP timeout, connection reset) with
   exponential backoff.
7. Surfaces 401s as `TokenExpiredException` so callers can distinguish
   refresh-worthy auth failures from permanent errors.

From the consumer's perspective, it's a standard JDBC driver. Everything that
reads from a regular database works — DBeaver, IntelliJ data source, JDBC tools,
your own Kotlin/Java code.

## How `ofload` positions itself

`ofjdbc` is the driver. `ofload` is the demonstration of what you can build
on top of it in ~2 500 lines of Kotlin:

- **Production-grade pipeline**: streaming, tuple-seek pagination, MERGE
  idempotency, split run-state.
- **Operational surface**: `/metrics` with Prometheus, structured logs with
  MDC, health/ready/live endpoints.
- **Zero visual flows**: the whole integration is git-diffable text.
- **One binary, one JVM, one process** — no adapter, no file landing zone, no
  orchestrator.

If you're evaluating `ofjdbc` for your own integration, this repo is the
reference you fork. Strip the dashboard, swap the watermark strategy, point
it at a different target — the core JDBC pattern holds.

## Cost profile

`ofload` is a single JVM process with bounded memory. A million-row table
consumes the same ~256 MB heap as a thousand-row one — so it runs anywhere:

- OCI Always Free (E2.1.Micro or Ampere A1 Flex) — **$0/mo**.
- $5 DigitalOcean droplet / Hetzner CX11 / equivalent.
- 250m CPU / 512 MB k8s pod inside a cluster you already pay for.
- On-prem Linux box next to everything else.

Compared to the alternatives, the cost math is one-sided:

| | OIC-based | BICC-based | `ofload` |
|---|---|---|---|
| Per-message / per-row licensing | scales with volume | — | none |
| Object Storage intermediate | — | $0.023/GB/mo + egress | none |
| Downstream loader (ODI / OCI DI) | — | separately licensed | — |
| Orchestrator / scheduler | included in OIC | separate | external (cron / k8s / OIC / …) |
| Compute | OIC bill | VM for the loader | 1 small VM |

For an org already running Fusion + ATP, the marginal cost of `ofload` is
just the small VM it runs on. The alternatives frequently run one or two
orders of magnitude higher for the same data volume.

Details in [operations.md](operations.md#where-this-runs-and-why-its-cheap).

## When to choose what

| Use case | Best fit |
|---|---|
| Batch warehouse replication from Fusion | `ofload` / ofjdbc |
| Event-driven, non-Oracle systems integration | OIC |
| Oracle Analytics Cloud as the direct consumer | BICC |
| B2B (EDI/AS2/FTP) with Oracle backend | OIC |
| One-off data dump for a migration project | BICC or DBeaver + ofjdbc |
| CI/CD-friendly, version-controlled pipelines | `ofload` / ofjdbc |

For anything in the "code-first ETL" quadrant, `ofjdbc` is the access layer
that makes the JDBC universe work against Fusion. `ofload` is what a solid
pipeline on top looks like.
