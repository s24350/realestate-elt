-- 02_clean_boe.sql
-- Clean Bank of England bronze.boe_raw -> silver.boe_monthly_clean
-- All numeric columns are converted; non-numeric tokens (e.g. 'n/a') become NULL.
-- date column is kept as DATE (bronze has it as DATE already).

CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.boe_monthly_clean CASCADE;

-- Create cleaned monthly table. CAST only when value looks numeric; else NULL.
CREATE TABLE silver.boe_monthly_clean AS
SELECT
  date AS date, -- already DATE in bronze
  -- pattern allows optional leading minus and decimals
  CASE WHEN trim(other_spec_remortgage) ~ '^[-]?[0-9]+(\.[0-9]+)?$' THEN trim(other_spec_remortgage)::numeric ELSE NULL END AS other_spec_remortgage,
  CASE WHEN trim(other_spec_other_lending) ~ '^[-]?[0-9]+(\.[0-9]+)?$' THEN trim(other_spec_other_lending)::numeric ELSE NULL END AS other_spec_other_lending,
  CASE WHEN trim(total_secured_lending) ~ '^[-]?[0-9]+(\.[0-9]+)?$' THEN trim(total_secured_lending)::numeric ELSE NULL END AS total_secured_lending,
  CASE WHEN trim(mfi_remortgage) ~ '^[-]?[0-9]+(\.[0-9]+)?$' THEN trim(mfi_remortgage)::numeric ELSE NULL END AS mfi_remortgage,
  CASE WHEN trim(mfi_other_lending) ~ '^[-]?[0-9]+(\.[0-9]+)?$' THEN trim(mfi_other_lending)::numeric ELSE NULL END AS mfi_other_lending,
  CASE WHEN trim(mfi_house_purchase) ~ '^[-]?[0-9]+(\.[0-9]+)?$' THEN trim(mfi_house_purchase)::numeric ELSE NULL END AS mfi_house_purchase,
  CASE WHEN trim(total_remortgage) ~ '^[-]?[0-9]+(\.[0-9]+)?$' THEN trim(total_remortgage)::numeric ELSE NULL END AS total_remortgage,
  CASE WHEN trim(total_other_lending) ~ '^[-]?[0-9]+(\.[0-9]+)?$' THEN trim(total_other_lending)::numeric ELSE NULL END AS total_other_lending,
  CASE WHEN trim(total_house_purchase) ~ '^[-]?[0-9]+(\.[0-9]+)?$' THEN trim(total_house_purchase)::numeric ELSE NULL END AS total_house_purchase,
  CASE WHEN trim(other_spec_house_purchase) ~ '^[-]?[0-9]+(\.[0-9]+)?$' THEN trim(other_spec_house_purchase)::numeric ELSE NULL END AS other_spec_house_purchase,
  CASE WHEN trim(mfi_total_approvals) ~ '^[-]?[0-9]+(\.[0-9]+)?$' THEN trim(mfi_total_approvals)::numeric ELSE NULL END AS mfi_total_approvals,
  CASE WHEN trim(other_spec_total_approvals) ~ '^[-]?[0-9]+(\.[0-9]+)?$' THEN trim(other_spec_total_approvals)::numeric ELSE NULL END AS other_spec_total_approvals,
  -- convenience fields for time-series joins
  date_trunc('month', date) AS month_start,
  date_trunc('quarter', date) AS quarter_start,
  extract(year from date)::int AS year,
  extract(month from date)::int AS month
FROM bronze.boe_raw;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_boe_date ON silver.boe_monthly_clean (date);
CREATE INDEX IF NOT EXISTS idx_boe_quarter ON silver.boe_monthly_clean (quarter_start);

-- Quick DQ queries to run manually:
-- 1) how many non-numeric tokens remain in each source column?
-- SELECT
--   COUNT(*) FILTER (WHERE other_spec_remortgage IS NULL AND trim((SELECT other_spec_remortgage FROM bronze.boe_raw LIMIT 1)) !~ '^[0-9]') AS bad_x
-- from bronze...  -- (use the ad-hoc queries you already ran)