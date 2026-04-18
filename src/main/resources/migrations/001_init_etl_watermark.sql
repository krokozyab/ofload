-- Baseline ETL_WATERMARK schema — run-state split with current/last-success/last-failure
-- sections plus live progress counter (CURRENT_RUN_ROWS) and last successful run duration.
--
-- Wrapped in a PL/SQL block with exception handler so existing deployments (where the
-- table was created manually via ddl/00_etl_watermark.sql earlier) see this migration
-- as a no-op: ORA-00955 "name already used" is swallowed, the migration is marked
-- applied, and any schema drift is patched by subsequent ALTER TABLE migrations.
BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE ETL_WATERMARK (
            TARGET_NAME              VARCHAR2(128)  PRIMARY KEY,
            WM_COLUMN                VARCHAR2(128)  NOT NULL,
            LAST_WM                  VARCHAR2(64),
            CURRENT_RUN_STARTED      TIMESTAMP,
            CURRENT_RUN_ID           VARCHAR2(64),
            CURRENT_RUN_ROWS         NUMBER DEFAULT 0,
            LAST_SUCCESS_FINISHED    TIMESTAMP,
            LAST_SUCCESS_ROWS        NUMBER DEFAULT 0,
            LAST_SUCCESS_DURATION_MS NUMBER,
            LAST_FAILURE_FINISHED    TIMESTAMP,
            LAST_FAILURE_ERROR       VARCHAR2(4000)
        )
    ';
EXCEPTION
    WHEN OTHERS THEN
        -- ORA-00955: name is already used by an existing object (table already exists).
        IF SQLCODE != -955 THEN RAISE; END IF;
END;
