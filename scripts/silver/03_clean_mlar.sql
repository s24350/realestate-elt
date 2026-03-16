-- 03_clean_mlar.sql
-- Unpivot the MLAR wide tables into one long silver.mlar_long table.
-- All quarter columns become rows; numeric parsing is applied (invalid -> NULL).
-- Source indicates which raw sheet the value came from.

CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.mlar_long CASCADE;

-- Combine three MLAR raw tables into one long table.
-- We use jsonb_each_text(to_jsonb(row) - 'category') to unpivot key/value pairs,
-- and filter keys that look like quarters (e.g. 2007Q1).

CREATE TABLE silver.mlar_long AS
SELECT
  src,
  category,
  key     AS quarter,        -- e.g. '2007Q1'
  CASE WHEN value ~ '^[-]?[0-9]+(\.[0-9]+)?$' THEN value::numeric ELSE NULL END AS value
FROM (
  SELECT '1.21' AS src, category, to_jsonb(t) - 'category' AS j
  FROM bronze.mlar_1_21_raw t
  UNION ALL
  SELECT '1.32' AS src, category, to_jsonb(t) - 'category' AS j
  FROM bronze.mlar_1_32_raw t
  UNION ALL
  SELECT '1.33' AS src, category, to_jsonb(t) - 'category' AS j
  FROM bronze.mlar_1_33_raw t
) s,
LATERAL jsonb_each_text(s.j) kv(key,value)
WHERE key ~ '^[0-9]{4}Q[1-4]$';

-- Add convenience columns and indexes
ALTER TABLE silver.mlar_long
  ADD COLUMN year int GENERATED ALWAYS AS (substring(quarter from 1 for 4)::int) STORED,
  ADD COLUMN quarter_num int GENERATED ALWAYS AS (substring(quarter from 6 for 1)::int) STORED,
  ADD COLUMN quarter_start date GENERATED ALWAYS AS (make_date(substring(quarter from 1 for 4)::int, ( ( (substring(quarter from 6 for 1)::int - 1) * 3) + 1 ), 1)) STORED;

CREATE INDEX IF NOT EXISTS idx_mlar_quarter_start ON silver.mlar_long (quarter_start);
CREATE INDEX IF NOT EXISTS idx_mlar_src_cat ON silver.mlar_long (src, category);

-- Optional: load mapping CSVs into silver.mlar_mapping (if you keep config files under /scripts/preprocessing/mappings)
-- If mappings exist and you want them available in DB, uncomment and run the COPY commands (file paths assume /scripts is mounted).
-- DROP TABLE IF EXISTS silver.mlar_mapping;
-- CREATE TABLE silver.mlar_mapping (src text, id text, label text);
-- COPY silver.mlar_mapping FROM '/scripts/preprocessing/mappings/1_21.csv' CSV HEADER;
-- COPY silver.mlar_mapping FROM '/scripts/preprocessing/mappings/1_32.csv' CSV HEADER;
-- COPY silver.mlar_mapping FROM '/scripts/preprocessing/mappings/1_33.csv' CSV HEADER;

-- Quick DQ checks:
-- 1) Count of NULL values (missing / invalid) by quarter
-- SELECT quarter, COUNT(*) FILTER (WHERE value IS NULL) AS num_nulls, COUNT(*) AS total_rows FROM silver.mlar_long GROUP BY quarter ORDER BY quarter;
-- 2) Sample categories with many NULLs
-- SELECT category, COUNT(*) FILTER (WHERE value IS NULL) AS nulls, COUNT(*) AS total FROM silver.mlar_long GROUP BY category HAVING COUNT(*) FILTER (WHERE value IS NULL) > 0 ORDER BY nulls DESC LIMIT 20;