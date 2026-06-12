"""
fdic_01_bronze.py
DAG 1: Bronze ingestion — synthetic FDIC data to GCS Parquet
Schedule: Quarterly (1st of Jan/Apr/Jul/Oct at 2am)
Triggers: fdic_02_silver on success
"""
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────
PROJECT = "gp-ct-sbox-con-gcp07f-de"
REGION  = "us-central1"
BUCKET  = "fdic-banking-pipeline-swagath"
CLUSTER = "fdic-bronze-cluster"

# ── Cluster config ────────────────────────────────────────────
# Mirrors the gcloud CLI flags from Session 2:
# --single-node --master-machine-type=n1-standard-4
# --image-version=2.1-debian11 --optional-components=HIVE_WEBHCAT
# --no-address (internal_ip_only=True)
# --max-idle=30m (idle_delete_ttl 1800 seconds)
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
    "software_config": {
        "image_version": "2.1-debian11",
        "optional_components": ["HIVE_WEBHCAT"],
    },
    "gce_cluster_config": {
        "internal_ip_only": True,   # --no-address equivalent
    },
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": 1800},  # 30-min idle safety net
    },
}

# ── PySpark job config ────────────────────────────────────────
BRONZE_JOB = {
    "reference": {"project_id": PROJECT},
    "placement": {"cluster_name": CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET}/scripts/fdic_bronze_ingest.py",
    },
}

# ── DAG definition ────────────────────────────────────────────
default_args = {
    "owner": "swagath",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="fdic_01_bronze",
    default_args=default_args,
    description="FDIC Bronze ingestion — synthetic data to GCS Parquet",
    schedule_interval="0 2 1 */3 *",   # quarterly: Jan/Apr/Jul/Oct 1st at 2am
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fdic", "bronze", "ingestion"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER,
    )

    run_bronze = DataprocSubmitJobOperator(
        task_id="run_bronze_ingest",
        job=BRONZE_JOB,
        region=REGION,
        project_id=PROJECT,
    )

    # trigger_rule=all_done: delete cluster even if job fails
    # CRITICAL for cost control — never leave cluster running
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT,
        cluster_name=CLUSTER,
        region=REGION,
        trigger_rule="all_done",
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="fdic_02_silver",
        wait_for_completion=False,  # Bronze DAG completes; Silver runs independently
    )

    # ── Task dependency chain ─────────────────────────────────
    create_cluster >> run_bronze >> delete_cluster >> trigger_silver
