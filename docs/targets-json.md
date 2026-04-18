# `targets.json` reference

Every pipeline definition lives in `src/main/resources/targets.json`. The file
is loaded once at service startup, validated statically, and held immutable in
memory for the process lifetime. To change a target, edit this file and
restart the service.

The format is a simple JSON object:

```json
{
  "targets": [
    { /* TargetConfig #1 */ },
    { /* TargetConfig #2 */ }
  ]
}
```

## Field reference

| Field | Type | Required | Default | Description |
|---|---|:---:|---|---|
| `name` | string | ✓ | — | Unique identifier. Used as `TARGET_NAME` in `ETL_WATERMARK`, in API paths (`POST /run/target/{name}`), in log MDC, and in Prometheus label `{target=…}`. |
| `stagingTable` | string | ✓ | — | Fully-qualified `STG_*` table name in ATP. Truncated at the start of every page. |
| `targetTable` | string | ✓ | — | Fully-qualified `T_*` table name in ATP — the final destination. |
| `watermarkColumn` | string | ✓ | — | The column (in both source and staging) that tracks incremental progress. Must appear in `sourceQuery`'s SELECT list. |
| `watermarkType` | enum | ✓ | — | `"TIMESTAMP"` or `"NUMBER"`. Determines which typed column (`LAST_WM_TIMESTAMP` vs `LAST_WM_NUMBER`) holds the watermark and how `initialWm` is parsed. |
| `keyColumns` | string[] | ✓ | — | Columns that uniquely identify a row. Used by tuple-seek pagination and by `targetMerge`'s `ON` clause. Composite keys (2+ columns) are supported. |
| `sourceQuery` | string | ✓ | — | `SELECT` against Fusion. Must contain the literal `/*WM*/` marker where the watermark predicate will be injected. See [SQL injection points](#sql-injection-points). |
| `targetInsert` | string | ✓ | — | Parameterised `INSERT` into `stagingTable`. One `?` per source column, in the same order as the SELECT list. |
| `targetMerge` | string | ✓ | — | Full `MERGE` statement `stagingTable → targetTable`. Executed verbatim after each successful page. |
| `initialWm` | string | ✓ | — | Watermark value used on the very first run (when no row exists in `ETL_WATERMARK`). For TIMESTAMP: `"yyyy-MM-dd HH:mm:ss[.fraction]"` or `"1970-01-01 00:00:00"` for full load. For NUMBER: any numeric string. |
| `pageSize` | int | — | null | Rows per page. `null` disables pagination (one-shot load). Typical production value: `10000`. |
| `lookbackMinutes` | long | — | null | Rewind the watermark by this many minutes on the first page of each run, to catch late-arriving updates. Safe under MERGE idempotency. |
| `group` | int | — | null | Dependency-ordering primitive. Targets in the same group run in parallel; group N+1 waits for group N. `null` → the target runs as its own singleton group. |
| `nlsSettings` | object | — | null | Per-target `ALTER SESSION SET NLS_*` settings applied on the staging connection before inserting. Useful when staging columns are typed and the source delivers strings. |

## SQL injection points

The three SQL fields — `sourceQuery`, `targetInsert`, `targetMerge` — are
executed verbatim. The pipeline performs **no SQL generation or rewriting**
other than substituting the `/*WM*/` marker in `sourceQuery`. This keeps the
executed SQL transparent: what you read in `targets.json` is what hits the DB.

### `sourceQuery` with the `/*WM*/` marker

```sql
SELECT col1, col2, ... FROM SOURCE_TABLE WHERE /*WM*/
```

The pipeline replaces `/*WM*/` per page:

- **First page** → `watermarkColumn > ?`
- **Subsequent pages (single key)** → `(watermarkColumn > ? OR (watermarkColumn = ? AND keyCol > ?))`
- **Subsequent pages (composite key)** → `(watermarkColumn > ? OR (watermarkColumn = ? AND (k1, k2, ...) > (?, ?, ...)))`

An `ORDER BY watermarkColumn, keyCol...` is appended automatically when
`pageSize` is set. The ofjdbc driver then appends its own `OFFSET … FETCH NEXT
pageSize ROWS ONLY` per WSDL round-trip.

The startup validator rejects `sourceQuery` that:
- Does not begin with `SELECT`.
- Does not contain `/*WM*/`.
- Has a SELECT column list not including `watermarkColumn` or any
  `keyColumns`.

### `targetInsert`

Must:
- Reference `stagingTable`.
- Have the same number of `?` placeholders as columns in the SELECT list.
- Have an explicit parenthesised column list (`INSERT INTO X (col1, col2) VALUES (?, ?)`)
  — the `INSERT INTO X VALUES (?,?)` shortcut is rejected by the validator.

Typical form:

```sql
INSERT /*+ APPEND_VALUES */ INTO STG_X (col1, col2, col3) VALUES (?, ?, ?)
```

The `APPEND_VALUES` hint is recommended but optional — it triggers direct-path
insert which bypasses the buffer cache.

### `targetMerge`

Must reference both `stagingTable` and `targetTable`. Typical form:

```sql
MERGE INTO T_X tgt USING STG_X src
  ON (tgt.PK = src.PK)
WHEN MATCHED THEN
  UPDATE SET tgt.col1 = src.col1, tgt.col2 = src.col2, ...
WHEN NOT MATCHED THEN
  INSERT (PK, col1, col2, ...) VALUES (src.PK, src.col1, src.col2, ...)
```

The pipeline does not parse or rewrite this SQL — you control the MERGE
semantics (e.g. you can filter rows, add `DELETE WHERE`, etc.).

## Watermark semantics

### First run

When `ETL_WATERMARK` has no row for this target, the watermark defaults to
`initialWm`. Choose this value carefully:

- `"1970-01-01 00:00:00"` for TIMESTAMP → loads everything (full history).
- `"0"` for NUMBER → same.
- A more recent date → only loads from that point forward, useful if you're
  bootstrapping a table you don't need the full history for.

### Incremental runs

Each subsequent run reads only rows with `watermarkColumn > LAST_WM`. The
watermark advances after each successful page commit — so a crash mid-run
leaves the pipeline resumable from the last committed page, not from scratch.

### Tuple-seek pagination

The key insight: `watermarkColumn > :wm` is unsafe at a page boundary when
multiple rows share the same watermark value. If row #10 000 has
`UPDATED_AT = '2024-08-09 16:46:14.609'` and rows #10 001…#10 050 have the
same `UPDATED_AT`, a naive next page using `> '2024-08-09 16:46:14.609'` would
skip them entirely.

Tuple-seek pagination avoids this by pairing the watermark with the key:

```sql
WHERE (UPDATED_AT > :wm OR (UPDATED_AT = :wm AND KEY > :last_key))
```

— strictly greater than the last row seen. Correct even under watermark ties.

## Lookback window

`lookbackMinutes` subtracts the configured value from the persisted watermark
on the first page of each run:

```
effective_wm = LAST_WM - lookbackMinutes
```

This re-scans recent rows on every run, which matters for sources that
update rows with timestamps slightly behind wall-clock time. Fusion modules
that often benefit:

| Module | Suggested `lookbackMinutes` | Rationale |
|---|---|---|
| GL journals | 60 | End-of-period posting can update hours of rows |
| AP invoices | 60 | Async approval workflow |
| XLA / subledger | 120 | Deferred SLA accounting runs |
| HR / payroll | 240–1440 | Batch payroll jobs may update historical rows |
| Static reference data | 0 / unset | No lookback needed |

MERGE idempotency makes re-processing safe — duplicates from overlapping
windows just overwrite the existing target row.

Lookback is applied **only on the first page** of a run. Subsequent pages use
the tuple-seek cursor and don't rewind.

## Groups — parallelism + ordering

`group` is both a parallelism hint and a sequential barrier:

```json
{ "name": "gl_headers",  "group": 1 },
{ "name": "ap_invoices", "group": 1 },
{ "name": "xla_lines",   "group": 2 }
```

Execution:

1. `gl_headers` and `ap_invoices` start in parallel on the shared ETL pool.
2. Both must complete (OK or FAILED) before `xla_lines` is submitted.

Use cases:

- **Dimension-then-fact loading**: dimensions in group 1, facts that join them in group 2.
- **Cross-table consistency**: load related tables together so downstream reports see a coherent snapshot.
- **Resource isolation**: isolate a heavy target into its own group to keep the ETL pool free for others.

Targets without a `group` run as singletons in ordering — effectively serial
with each other, but any group with `group: 1` still runs first.

## `nlsSettings`

Per-target NLS session parameters applied on the staging connection before
insert. Useful when staging columns are typed (`DATE`, `TIMESTAMP`, `NUMBER`)
and the source returns strings in a specific format.

```json
"nlsSettings": {
  "NLS_DATE_FORMAT": "YYYY-MM-DD",
  "NLS_TIMESTAMP_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF",
  "NLS_NUMERIC_CHARACTERS": ".,"
}
```

The typical case is "all VARCHAR2 staging, Oracle converts on INSERT" — for
which the baseline set by `OracleDs.getConnection` already covers most
scenarios. Override per target only when your target has unusual
format expectations.

## Full annotated example

```json
{
  "targets": [
    {
      "name": "ap_invoices",
      "stagingTable": "STG_AP_INVOICES",
      "targetTable":  "T_AP_INVOICES",

      "watermarkColumn": "LAST_UPDATE_DATE",
      "watermarkType":   "TIMESTAMP",
      "keyColumns":      ["INVOICE_ID"],

      "sourceQuery":  "SELECT INVOICE_ID, LAST_UPDATE_DATE, VENDOR_ID, INVOICE_NUM, INVOICE_AMOUNT FROM AP_INVOICES_ALL WHERE /*WM*/",
      "targetInsert": "INSERT /*+ APPEND_VALUES */ INTO STG_AP_INVOICES (INVOICE_ID, LAST_UPDATE_DATE, VENDOR_ID, INVOICE_NUM, INVOICE_AMOUNT) VALUES (?, ?, ?, ?, ?)",
      "targetMerge":  "MERGE INTO T_AP_INVOICES tgt USING STG_AP_INVOICES src ON (tgt.INVOICE_ID = src.INVOICE_ID) WHEN MATCHED THEN UPDATE SET tgt.LAST_UPDATE_DATE = src.LAST_UPDATE_DATE, tgt.VENDOR_ID = src.VENDOR_ID, tgt.INVOICE_NUM = src.INVOICE_NUM, tgt.INVOICE_AMOUNT = src.INVOICE_AMOUNT WHEN NOT MATCHED THEN INSERT (INVOICE_ID, LAST_UPDATE_DATE, VENDOR_ID, INVOICE_NUM, INVOICE_AMOUNT) VALUES (src.INVOICE_ID, src.LAST_UPDATE_DATE, src.VENDOR_ID, src.INVOICE_NUM, src.INVOICE_AMOUNT)",

      "initialWm":        "1970-01-01 00:00:00",
      "pageSize":         10000,
      "lookbackMinutes":  60,
      "group":            1,

      "nlsSettings": {
        "NLS_DATE_FORMAT":        "YYYY-MM-DD",
        "NLS_TIMESTAMP_FORMAT":   "YYYY-MM-DD HH24:MI:SS.FF",
        "NLS_NUMERIC_CHARACTERS": ".,"
      }
    }
  ]
}
```

## Composite-key example

For tables whose primary key is composed of multiple columns:

```json
{
  "name": "ap_invoice_lines",
  "keyColumns": ["INVOICE_ID", "LINE_NUMBER"],

  "sourceQuery": "SELECT INVOICE_ID, LINE_NUMBER, LAST_UPDATE_DATE, AMOUNT FROM AP_INVOICE_LINES_ALL WHERE /*WM*/",

  "targetMerge": "MERGE INTO T_AP_INVOICE_LINES tgt USING STG_AP_INVOICE_LINES src ON (tgt.INVOICE_ID = src.INVOICE_ID AND tgt.LINE_NUMBER = src.LINE_NUMBER) WHEN MATCHED THEN ..."
}
```

The tuple-seek page predicate becomes:

```sql
WHERE (LAST_UPDATE_DATE > ? OR (LAST_UPDATE_DATE = ? AND (INVOICE_ID, LINE_NUMBER) > (?, ?)))
ORDER BY LAST_UPDATE_DATE, INVOICE_ID, LINE_NUMBER
```

Row-value comparison in Oracle is strictly lexicographic, matching what the
`ORDER BY` produces — so the cursor advance is correct.

## Validation at startup

The entire config is validated before the HTTP server comes up. Typical
errors (all emitted at once, not short-circuited):

```
targets.json validation failed with 3 error(s):
  [ap_invoices] sourceQuery must contain '/*WM*/' marker ...
  [ap_invoices] watermarkColumn 'LAST_UPDATE_DATE' is not in sourceQuery SELECT list
  [ap_invoices] targetInsert column count (4) does not match sourceQuery SELECT count (5) — will crash on first row
```

The service refuses to start with a nonzero validation result. Fix the issues,
restart.
