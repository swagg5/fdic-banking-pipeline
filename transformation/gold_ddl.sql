-- =============================================================================
-- FDIC Bank Financial Intelligence Pipeline
-- Gold Layer DDL — BigQuery
-- Session 3 | Project: gp-ct-sbox-con-gcp07f-de | Dataset: fdic_gold
-- =============================================================================
-- Execution order:
--   1. Run Step 1 (bq load) from Cloud Shell — not SQL
--   2. Run Step 2 (bq load) from Cloud Shell — not SQL
--   3. Run Steps 3-6 in BigQuery console or via bq query
-- =============================================================================


-- =============================================================================
-- STEP 1: Load institutions_raw from Silver GCS (run in Cloud Shell)
-- =============================================================================
-- bq load \
--   --source_format=PARQUET \
--   --autodetect \
--   fdic_gold.institutions_raw \
--   "gs://fdic-banking-pipeline-swagath/silver/institutions/part-*.snappy.parquet"


-- =============================================================================
-- STEP 2: Load financials_raw from Silver GCS (run in Cloud Shell)
-- =============================================================================
-- for year in 2023 2024; do
--   for q in Q1 Q2 Q3 Q4; do
--     bq load --source_format=PARQUET --autodetect fdic_gold.financials_raw \
--       "gs://fdic-banking-pipeline-swagath/silver/financials/year=${year}/quarter=${q}/part-*.snappy.parquet"
--   done
-- done

-- Fix if year/quarter columns missing (derive from repdte):
-- CREATE OR REPLACE TABLE fdic_gold.financials_raw AS
-- SELECT *,
--   CAST(SUBSTR(repdte,1,4) AS INT64) AS year,
--   CAST(CEIL(CAST(SUBSTR(repdte,5,2) AS FLOAT64) / 3) AS INT64) AS quarter
-- FROM fdic_gold.financials_raw;


-- =============================================================================
-- STEP 3: bank_health_metrics — Core Gold serving table
-- Partitioned by year (integer range), clustered by stalp
-- Capital adequacy flags follow Basel III thresholds
-- =============================================================================

CREATE OR REPLACE TABLE fdic_gold.bank_health_metrics
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2020, 2030, 1))
CLUSTER BY stalp
OPTIONS (description="Bank financial health metrics with Basel III capital adequacy classification") AS

SELECT
  f.cert,
  i.name,
  i.city,
  i.stalp,
  f.repdte,
  f.year,
  f.quarter,
  f.asset,
  f.dep,
  f.lnlsnet,
  f.rbct1,

  -- Derived financial health metrics
  ROUND(f.nperfv / f.asset * 100, 2)  AS npl_ratio_pct,       -- Non-performing loan ratio
  ROUND(f.netinc / f.asset * 100, 4)  AS roa_pct,             -- Return on assets
  ROUND(f.dep    / f.asset * 100, 2)  AS deposit_to_asset_pct,-- Deposit funding ratio

  -- Basel III capital adequacy classification
  -- Well Capitalised >= 10%, Adequately >= 8%, Undercapitalised >= 6%
  CASE
    WHEN f.rbct1 >= 10 THEN 'Well Capitalised'
    WHEN f.rbct1 >= 8  THEN 'Adequately Capitalised'
    WHEN f.rbct1 >= 6  THEN 'Undercapitalised'
    ELSE 'Critically Undercapitalised'
  END AS capital_adequacy_flag,

  CURRENT_TIMESTAMP() AS _loaded_at

FROM fdic_gold.financials_raw  f
LEFT JOIN fdic_gold.institutions_raw i ON f.cert = i.cert;


-- =============================================================================
-- STEP 4: dim_institution — Institution dimension (SCD Type 2 pattern)
-- Single snapshot: all records current (effective_date = repdte, expiry = 99991231)
-- In production: new snapshots would add historical versions per bank
-- =============================================================================

CREATE OR REPLACE TABLE fdic_gold.dim_institution
CLUSTER BY cert, stalp
OPTIONS (description="Institution dimension — SCD Type 2 with effective/expiry dates") AS

SELECT
  cert,
  name,
  city,
  stalp,
  instcat,
  charterclass,
  active,
  repdte      AS effective_date,   -- When this version became active
  '99991231'  AS expiry_date,      -- 9999-12-31 = current record (no expiry)
  TRUE        AS is_current,
  CURRENT_TIMESTAMP() AS row_created_at
FROM fdic_gold.institutions_raw;


-- =============================================================================
-- STEP 5: dq_scorecard — DQ audit trail table
-- Partitioned by run_date to enable efficient trend queries
-- Every pipeline run inserts one row per dataset processed
-- =============================================================================

CREATE TABLE IF NOT EXISTS fdic_gold.dq_scorecard (
  run_id              STRING    OPTIONS(description="Pipeline run ID: YYYYMMDD_HHMMSS"),
  run_date            DATE      OPTIONS(description="Date the pipeline ran"),
  dataset             STRING    OPTIONS(description="Source dataset: institutions or financials"),
  pipeline_stage      STRING    OPTIONS(description="Stage: bronze_ingest or silver_transform"),
  total_records       INT64     OPTIONS(description="Total input records processed"),
  clean_records       INT64     OPTIONS(description="Records passing all DQ rules"),
  quarantine_records  INT64     OPTIONS(description="Records failing at least one DQ rule"),
  dq_pass_pct         FLOAT64   OPTIONS(description="Pass rate as percentage"),
  created_at          TIMESTAMP OPTIONS(description="Row creation timestamp")
)
PARTITION BY run_date
OPTIONS (
  description = "DQ audit trail — tracks pass/fail rates per pipeline run over time"
);


-- =============================================================================
-- STEP 6: Insert Session 2 Silver transformation run stats into dq_scorecard
-- =============================================================================

INSERT INTO fdic_gold.dq_scorecard
  (run_id, run_date, dataset, pipeline_stage,
   total_records, clean_records, quarantine_records, dq_pass_pct, created_at)
VALUES
  ('20260610_141234', DATE '2026-06-10', 'institutions', 'silver_transform',
    500, 500, 0, 100.0, CURRENT_TIMESTAMP()),
  ('20260610_141234', DATE '2026-06-10', 'financials', 'silver_transform',
    4000, 4000, 0, 100.0, CURRENT_TIMESTAMP());


-- =============================================================================
-- VERIFICATION QUERIES — Run after all steps complete
-- =============================================================================

-- Table inventory (metadata view)
SELECT table_id, row_count, size_bytes / 1024 AS size_kb
FROM `fdic_gold.__TABLES__`
ORDER BY table_id;

-- Capital adequacy distribution (regulatory insight)
SELECT
  capital_adequacy_flag,
  COUNT(*)           AS bank_quarters,
  ROUND(AVG(rbct1),2) AS avg_t1_ratio,
  ROUND(AVG(npl_ratio_pct),3) AS avg_npl_pct
FROM fdic_gold.bank_health_metrics
WHERE year = 2024
GROUP BY 1
ORDER BY 1;

-- Quarter-over-quarter Tier 1 ratio trend
SELECT
  year,
  quarter,
  COUNT(DISTINCT cert)        AS bank_count,
  ROUND(AVG(rbct1),2)         AS avg_tier1_ratio,
  ROUND(AVG(npl_ratio_pct),3) AS avg_npl_pct
FROM fdic_gold.bank_health_metrics
GROUP BY 1, 2
ORDER BY 1, 2;

-- Regional analysis (clustering benefit — fast filter on stalp)
SELECT
  stalp,
  COUNT(*)           AS bank_count,
  ROUND(AVG(rbct1),2) AS avg_t1
FROM fdic_gold.bank_health_metrics
WHERE year = 2024 AND quarter = 4
GROUP BY stalp
ORDER BY bank_count DESC;

-- SCD Type 2 structure verification
SELECT cert, name, effective_date, expiry_date, is_current
FROM fdic_gold.dim_institution
WHERE is_current = TRUE
LIMIT 5;

-- DQ scorecard audit trail
SELECT run_id, dataset, total_records, clean_records, dq_pass_pct
FROM fdic_gold.dq_scorecard
ORDER BY dataset;
