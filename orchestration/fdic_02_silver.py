"""
fdic_02_silver.py
DAG 2: Silver transformation — PySpark DQ, quarantine, Hive tables
Schedule: None (triggered by fdic_01_bronze)
Triggers: fdic_03_gold on success
"""
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

PROJECT = "gp-ct-sbox-con-gcp07f-de"
REGION  = "us-central1"
BUCKET  = "fdic-banking-pipeline-swagath"
CLUSTER = "fdic-silver-cluster"

# Same cluster config as DAG 1 — consistent spec across pipeline
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
        "internal_ip_only": True,
    },
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": 1800},
    },
}

# Spark properties passed directly to the PySpark job
# Mirrors what we set in SparkSession.builder.config() in Session 2
SILVER_JOB = {
    "reference": {"project_id": PROJECT},
    "placement": {"cluster_name": CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET}/scripts/silver_transform.py",
        "properties": {
            "spark.sql.warehouse.dir":
                f"gs://{BUCKET}/hive-warehouse",
            "spark.hadoop.hive.metastore.warehouse.dir":
                f"gs://{BUCKET}/hive-warehouse",
            "spark.sql.legacy.allowNonEmptyLocationInCTAS": "true",
        },
    },
}

default_args = {
    "owner": "swagath",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="fdic_02_silver",
    default_args=default_args,
    description="FDIC Silver transformation — DQ, quarantine, Hive registration",
    schedule_interval=None,   # triggered by fdic_01_bronze, not on schedule
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fdic", "silver", "transform"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER,
    )

    run_silver = DataprocSubmitJobOperator(
        task_id="run_silver_transform",
        job=SILVER_JOB,
        region=REGION,
        project_id=PROJECT,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT,
        cluster_name=CLUSTER,
        region=REGION,
        trigger_rule="all_done",
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="fdic_03_gold",
        wait_for_completion=False,
    )

    create_cluster >> run_silver >> delete_cluster >> trigger_gold
