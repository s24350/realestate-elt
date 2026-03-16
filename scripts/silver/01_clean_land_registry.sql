-- 01_clean_land_registry.sql
-- Create silver schema and produce quarterly aggregates from bronze.land_registry_raw

CREATE SCHEMA IF NOT EXISTS silver;

-- Drop previous clean tables if present (idempotent)
DROP TABLE IF EXISTS silver.land_registry_quarterly CASCADE;
DROP TABLE IF EXISTS silver.land_registry_by_type CASCADE;

-- Main quarterly aggregates (all property types together)
CREATE TABLE silver.land_registry_quarterly AS
WITH cleaned AS (
  SELECT
    -- date_of_transfer in bronze is a timestamp; keep using it
    date_trunc('quarter', date_of_transfer) AS quarter_start,
    price::numeric AS price -- price is integer in bronze; cast to numeric for aggregates
  FROM bronze.land_registry_raw
  WHERE price IS NOT NULL
)
SELECT
  quarter_start,
  to_char(quarter_start,'YYYY') || 'Q' || extract(quarter from quarter_start)::int AS quarter_label,
  COUNT(*)                                AS total_transactions,
  AVG(price)::numeric(18,2)               AS avg_price,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
  MIN(price)                              AS min_price,
  MAX(price)                              AS max_price
FROM cleaned
GROUP BY quarter_start
ORDER BY quarter_start;

-- Aggregates by property type (D,S,T,F,O)
CREATE TABLE silver.land_registry_by_type AS
SELECT
  date_trunc('quarter', date_of_transfer) AS quarter_start,
  to_char(date_trunc('quarter', date_of_transfer),'YYYY') || 'Q' || extract(quarter from date_trunc('quarter', date_of_transfer))::int AS quarter_label,
  property_type,
  COUNT(*)                                AS cnt,
  AVG(price::numeric)::numeric(18,2)      AS avg_price,
  MIN(price::numeric)                     AS min_price,
  MAX(price::numeric)                     AS max_price
FROM bronze.land_registry_raw
GROUP BY quarter_start, property_type
ORDER BY quarter_start, property_type;

-- Indexes for faster joins
CREATE INDEX IF NOT EXISTS idx_land_registry_quarter_start ON silver.land_registry_quarterly (quarter_start);
CREATE INDEX IF NOT EXISTS idx_land_registry_by_type_q ON silver.land_registry_by_type (quarter_start, property_type);

-- Basic DQ checks (run manually)
-- SELECT COUNT(*) FROM silver.land_registry_quarterly;
-- SELECT MIN(quarter_start), MAX(quarter_start) FROM silver.land_registry_quarterly;