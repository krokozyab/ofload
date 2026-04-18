-- ETL run coordination + watermark progress table.
--
-- Columns are split into three sections so the dashboard can show an honest view
-- of current state without fields visually conflicting:
--   * Progress:   LAST_WM advances per successful page within a run.
--   * Current:    CURRENT_RUN_* populated only while a run is in flight (all NULL when idle).
--   * Outcomes:   LAST_SUCCESS_* and LAST_FAILURE_* are independent records of the most
--                 recent OK and FAILED completions respectively. The later of the two
--                 timestamps tells the UI which outcome is "current" between runs.
CREATE TABLE ETL_WATERMARK (
    TARGET_NAME           VARCHAR2(128)  PRIMARY KEY,
    WM_COLUMN             VARCHAR2(128)  NOT NULL,
    LAST_WM               VARCHAR2(64),

    -- Current (in-flight) run; CURRENT_RUN_STARTED/ID are NULL when idle.
    -- CURRENT_RUN_ROWS is a running counter for the in-flight run (pages committed),
    -- so the UI can show live progress before the run finishes.
    CURRENT_RUN_STARTED   TIMESTAMP,
    CURRENT_RUN_ID        VARCHAR2(64),
    CURRENT_RUN_ROWS      NUMBER DEFAULT 0,

    -- Last OK completion.
    LAST_SUCCESS_FINISHED    TIMESTAMP,
    LAST_SUCCESS_ROWS        NUMBER DEFAULT 0,
    LAST_SUCCESS_DURATION_MS NUMBER,

    -- Last FAILED completion.
    LAST_FAILURE_FINISHED TIMESTAMP,
    LAST_FAILURE_ERROR    VARCHAR2(4000)
);
