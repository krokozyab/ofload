-- Add typed watermark columns alongside the legacy VARCHAR2 LAST_WM.
--
-- Rationale: storing a per-target watermark as VARCHAR2 means we rely on string
-- ordering matching time / numeric ordering, and on a consistent text format on
-- both read and write. Small format drift (extra fractional-second digits, a
-- surprising NLS setting) can silently break incremental loads. Typed columns
-- make Oracle enforce the format at write time and return a predictable type at
-- read time; the string conversion only happens once, at the boundary where we
-- bind into the source SELECT.
--
-- Exactly one of these is populated per row — chosen by the target's
-- watermarkType at write time in WatermarkStore. The legacy LAST_WM column is
-- left in place for now so old rows / tooling that still read it don't break;
-- it can be dropped in a later migration once everything is switched over.
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE ETL_WATERMARK ADD (LAST_WM_TIMESTAMP TIMESTAMP(6), LAST_WM_NUMBER NUMBER)';
EXCEPTION
    WHEN OTHERS THEN
        -- ORA-01430: column being added already exists in table (fresh schema from 001 updated later).
        IF SQLCODE != -1430 THEN RAISE; END IF;
END;
