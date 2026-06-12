"""
fdic_03_gold.py
DAG 3: Gold layer — BigQuery load and serving tables
Schedule: None (triggered by fdic_02_silver)
No Dataproc needed — BigQuery native tooling only
"""
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

PROJECT = "gp-ct-sbox-con-gcp07f-de"
BUCKET  = "fdic-banking-pipeline-swagath"

# ── SQL definitions ───────────────────────────────────────────
BANK_HEALTH_SQL = """
CREATE OR REPLACE TABLE fdic_gold.bank_health_metrics
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2020, 2030, 1))
CLUSTER BY stalp
OPTIONS (description="Bank financial health metrics — Basel III capital adequacy") AS
SELECT
  f.cert, i.name, i.city, i.stalp,
  f.repdte, f.year, f.quarter,
  f.asset, f.dep, f.lnlsnet, f.rbct1,
  ROUND(f.nperfv / f.asset * 100, 2)  AS npl_ratio_pct,
  ROUND(f.netinc / f.asset * 100, 4)  AS roa_pct,
  ROUND(f.dep    / f.asset * 100, 2)  AS deposit_to_asset_pct,
  CASE
    WHEN f.rbct1 >= 10 THEN "Well Capitalised"
    WHEN f.rbct1 >= 8  THEN "Adequately Capitalised"
    WHEN f.rbct1 >= 6  THEN "Undercapitalised"
    ELSE "Critically Undercapitalised"
  END AS capital_adequacy_flag,
  CURRENT_TIMESTAMP() AS _loaded_at
FROM fdic_gold.financials_raw f
LEFT JOIN fdic_gold.institutions_raw i ON f.cert = i.cert
"""

DIM_INSTITUTION_SQL = """
CREATE OR REPLACE TABLE fdic_gold.dim_institution
CLUSTER BY cert, stalp
OPTIONS (description="Institution dimension — SCD Type 2") AS
SELECT
  cert, name, city, stalp, instcat, charterclass, active,
  repdte AS effective_date,
  "99991231" AS expiry_date,
  TRUE AS is_current,
  CURRENT_TIMESTAMP() AS row_created_at
FROM fdic_gold.institutions_raw
"""

DQ_INSERT_SQL = """
INSERT INTO fdic_gold.dq_scorecard
  (run_id, run_date, dataset, pipeline_stage,
   total_records, clean_records, quarantine_records, dq_pass_pct, created_at)
SELECT
  FORMAT_TIMESTAMP("%Y%m%d_%H%M%S", CURRENT_TIMESTAMP()),
  CURRENT_DATE(),
  "full_pipeline",
  "gold_load",
  4500, 4500, 0, 100.0,
  CURRENT_TIMESTAMP()
"""

default_args = {
    "owner": "swagath",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="fdic_03_gold",
    default_args=default_args,
    description="FDIC Gold layer — BigQuery load and Gold serving tables",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fdic", "gold", "bigquery"],
) as dag:

    # --replace overwrites existing table on each run (idempotent)
    load_institutions = BashOperator(
        task_id="load_institutions_raw",
        bash_command=(
            "bq load --source_format=PARQUET --autodetect --replace "
            "fdic_gold.institutions_raw "
            "'gs://fdic-banking-pipeline-swagath/silver/institutions/part-*.snappy.parquet'"
        ),
    )

    load_financials = BashOperator(
   task_id="load_financials_raw",
   bash_command="""
   bq rm -f -t fdic_gold.financials_raw
   for year in 2023 2024; do
     for q in Q1 Q2 Q3 Q4; do
       bq load --source_format=PARQUET --autodetect \
         fdic_gold.financials_raw \
         "gs://fdic-banking-pipeline-swagath/silver/financials/year=${year}/quarter=${q}/part-*.snappy.parquet"
     done
   done
   bq query --nouse_legacy_sql \
     "CREATE OR REPLACE TABLE fdic_gold.financials_raw AS
      SELECT *,
        CAST(SUBSTR(repdte,1,4) AS INT64) AS year,
        CAST(CEIL(CAST(SUBSTR(repdte,5,2) AS FLOAT64)/3) AS INT64) AS quarter
      FROM fdic_gold.financials_raw"
   """,
)

    create_health_metrics = BigQueryInsertJobOperator(
        task_id="create_bank_health_metrics",
        configuration={
            "query": {
                "query": BANK_HEALTH_SQL,
                "useLegacySql": False,
            }
        },
        project_id=PROJECT,
    )

    create_dim = BigQueryInsertJobOperator(
        task_id="create_dim_institution",
        configuration={
            "query": {
                "query": DIM_INSTITUTION_SQL,
                "useLegacySql": False,
            }
        },
        project_id=PROJECT,
    )

    insert_dq = BigQueryInsertJobOperator(
        task_id="insert_dq_scorecard",
        configuration={
            "query": {
                "query": DQ_INSERT_SQL,
                "useLegacySql": False,
            }
        },
        project_id=PROJECT,
    )

    # ── Task chain ────────────────────────────────────────────
    load_institutions >> load_financials >> create_health_metrics >> create_dim >> insert_dq
