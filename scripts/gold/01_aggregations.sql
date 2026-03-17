-- scripts/gold/01_aggregations.sql

CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.housing_credit_summary;

CREATE TABLE gold.housing_credit_summary AS

WITH

lr AS (
    SELECT
        quarter_start::date AS quarter_start,
        total_transactions AS transactions_total__LR__count,
        avg_price AS price_avg__LR__gbp,
        median_price AS price_median__LR__gbp
    FROM silver.land_registry_quarterly
),

boe AS (
    SELECT
        date_trunc('quarter', date)::date AS quarter_start,

        SUM(total_secured_lending) AS boe_total_secured_lending__LPMB3C8__count,
        SUM(total_remortgage) AS boe_total_remortgage__LPMB4B3__count,
        SUM(total_other_lending) AS boe_total_other_lending__LPMB4B4__count,
        SUM(total_house_purchase) AS boe_house_purchase__LPMVTVX__count,
        SUM(mfi_total_approvals) AS boe_mfi_total_approvals__LPMZ3UP__count,

        SUM(
            (total_secured_lending IS NULL)::int +
            (total_remortgage IS NULL)::int +
            (total_other_lending IS NULL)::int +
            (total_house_purchase IS NULL)::int +
            (mfi_total_approvals IS NULL)::int
        ) AS dq_boe_nulls_count__meta

    FROM silver.boe_monthly_clean
    GROUP BY 1
),

mlar AS (
    SELECT
        quarter_start,

        MAX(CASE WHEN src='1.21' AND category = 'All (Regulated and Non-regulated) - Business flows - Gross advances'
            THEN value * 1000000 END)
            AS mlar_gross_advances__MLAR_1_21_C_1__gbp,

        MAX(CASE WHEN src='1.21' AND category = 'All (Regulated and Non-regulated) - Business flows - Net advances'
            THEN value * 1000000 END)
            AS mlar_net_advances__MLAR_1_21_C_2__gbp,

        MAX(CASE WHEN src='1.21' AND category = 'All (Regulated and Non-regulated) - Business flows - New commitments'
            THEN value * 1000000 END)
            AS mlar_new_commitments__MLAR_1_21_C_3__gbp,

        MAX(CASE WHEN src='1.32' AND category = 'All (Regulated and Non-regulated) - With Impaired credit history - By payment type - Advances - Repayment (capital + interest)'
            THEN value END)
            AS mlar_imp_repayment__MLAR_1_32_C_3__pct,

        MAX(CASE WHEN src='1.32' AND category = 'All (Regulated and Non-regulated) - With Impaired credit history - By payment type - Advances - Interest only'
            THEN value END)
            AS mlar_imp_interest_only__MLAR_1_32_C_4__pct,

        MAX(CASE WHEN src='1.32' AND category = 'All (Regulated and Non-regulated) - With Impaired credit history - By payment type - Advances - Combined'
            THEN value END)
            AS mlar_imp_combined__MLAR_1_32_C_5__pct,

        MAX(CASE WHEN src='1.32' AND category = 'All (Regulated and Non-regulated) - With Impaired credit history - By payment type - Advances - Other'
            THEN value END)
            AS mlar_imp_other__MLAR_1_32_C_6__pct,

        MAX(CASE WHEN src='1.33' AND category = 'All (Regulated and Non-regulated) - New commitments in quarter - (ii) Amounts by purpose - House purchase'
            THEN value * 1000000 END)
            AS mlar_new_house_purchase__MLAR_1_33_C_29__gbp,

        MAX(CASE WHEN src='1.33' AND category = 'All (Regulated and Non-regulated) - New commitments in quarter - (ii) Amounts by purpose - Remortgage'
            THEN value * 1000000 END)
            AS mlar_new_remortgage__MLAR_1_33_C_30__gbp,

        MAX(CASE WHEN src='1.33' AND category = 'All (Regulated and Non-regulated) - New commitments in quarter - (ii) Amounts by purpose - Other (inc further advances)'
            THEN value * 1000000 END)
            AS mlar_new_other__MLAR_1_33_C_31__gbp

    FROM silver.mlar_long
    GROUP BY quarter_start
)

-- final SELECT: explicit column list (no duplicate quarter_start)
SELECT
  COALESCE(lr.quarter_start, boe.quarter_start, mlar.quarter_start) AS quarter_start__date,

  -- LAND REGISTRY (explicit)
  lr.transactions_total__LR__count,
  lr.price_avg__LR__gbp,
  lr.price_median__LR__gbp,

  -- BOE (explicit)
  boe.boe_total_secured_lending__LPMB3C8__count,
  boe.boe_total_remortgage__LPMB4B3__count,
  boe.boe_total_other_lending__LPMB4B4__count,
  boe.boe_house_purchase__LPMVTVX__count,
  boe.boe_mfi_total_approvals__LPMZ3UP__count,
  boe.dq_boe_nulls_count__meta,

  -- MLAR (explicit)
  mlar.mlar_gross_advances__MLAR_1_21_C_1__gbp,
  mlar.mlar_net_advances__MLAR_1_21_C_2__gbp,
  mlar.mlar_new_commitments__MLAR_1_21_C_3__gbp,

  mlar.mlar_imp_repayment__MLAR_1_32_C_3__pct,
  mlar.mlar_imp_interest_only__MLAR_1_32_C_4__pct,
  mlar.mlar_imp_combined__MLAR_1_32_C_5__pct,
  mlar.mlar_imp_other__MLAR_1_32_C_6__pct,

  mlar.mlar_new_house_purchase__MLAR_1_33_C_29__gbp,
  mlar.mlar_new_remortgage__MLAR_1_33_C_30__gbp,
  mlar.mlar_new_other__MLAR_1_33_C_31__gbp,

  -- computed MLAR null-count (post-pivot)
  (
    (mlar.mlar_gross_advances__MLAR_1_21_C_1__gbp IS NULL)::int +
    (mlar.mlar_net_advances__MLAR_1_21_C_2__gbp IS NULL)::int +
    (mlar.mlar_new_commitments__MLAR_1_21_C_3__gbp IS NULL)::int +
    (mlar.mlar_new_house_purchase__MLAR_1_33_C_29__gbp IS NULL)::int +
    (mlar.mlar_new_remortgage__MLAR_1_33_C_30__gbp IS NULL)::int +
    (mlar.mlar_new_other__MLAR_1_33_C_31__gbp IS NULL)::int
  ) AS dq_mlar_nulls_count__meta,

  -- source flags
  (lr.quarter_start IS NOT NULL)   AS source_available_lr__flag,
  (boe.quarter_start IS NOT NULL)  AS source_available_boe__flag,
  (mlar.quarter_start IS NOT NULL) AS source_available_mlar__flag

FROM lr
FULL OUTER JOIN boe
  ON lr.quarter_start = boe.quarter_start
FULL OUTER JOIN mlar
  ON COALESCE(lr.quarter_start, boe.quarter_start) = mlar.quarter_start

ORDER BY quarter_start__date;