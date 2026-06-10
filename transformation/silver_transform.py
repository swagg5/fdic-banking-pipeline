"""
silver_transform.py

Reads Bronze Parquet from GCS, applies DQ validation,
routes bad records to quarantine, writes clean records to Silver.
Registers Hive external tables on Silver Parquet.

KEY FIX: Hive tables are dropped at START of main() — before Silver
is written — so DROP TABLE cannot delete Silver data even if previous
runs left managed tables behind.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

# ── Configuration ──────────────────────────────────────────────────────────────
BUCKET          = "fdic-banking-pipeline-swagath"
BRONZE_BASE     = f"gs://{BUCKET}/bronze"
SILVER_BASE     = f"gs://{BUCKET}/silver"
QUARANTINE_BASE = f"gs://{BUCKET}/quarantine"
RUN_ID          = datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def get_spark():
    return (SparkSession.builder
            .appName("fdic-silver-transform")
            .enableHiveSupport()
            .config("spark.sql.warehouse.dir",
                    f"gs://{BUCKET}/hive-warehouse")
            .config("spark.hadoop.hive.metastore.warehouse.dir",
                    f"gs://{BUCKET}/hive-warehouse")
            .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
            .getOrCreate())


# ── DQ FRAMEWORK ───────────────────────────────────────────────────────────────

def apply_institution_dq(df):
    df = df.withColumn("rejection_reason",
        F.concat_ws("; ",
            F.when(F.col("cert").isNull(),
                   F.lit("cert_is_null")),
            F.when(F.col("asset") <= 0,
                   F.lit("asset_non_positive")),
            F.when(F.col("name").isNull() | (F.trim(F.col("name")) == ""),
                   F.lit("name_empty")),
            F.when(~F.col("active").isin([0, 1]),
                   F.lit("active_invalid_value"))
        )
    )
    clean = df.filter(F.col("rejection_reason") == "").drop("rejection_reason")
    bad   = df.filter(F.col("rejection_reason") != "")
    return clean, bad


def apply_financials_dq(df):
    df = df.withColumn("rejection_reason",
        F.concat_ws("; ",
            F.when(F.col("cert").isNull(),
                   F.lit("cert_is_null")),
            F.when(F.col("asset") <= 0,
                   F.lit("asset_non_positive")),
            F.when(F.col("dep") <= 0,
                   F.lit("dep_non_positive")),
            F.when((F.col("rbct1") < 0) | (F.col("rbct1") > 100),
                   F.lit("rbct1_out_of_range")),
            F.when(F.length(F.col("repdte")) != 8,
                   F.lit("repdte_invalid_format"))
        )
    )
    clean = df.filter(F.col("rejection_reason") == "").drop("rejection_reason")
    bad   = df.filter(F.col("rejection_reason") != "")
    return clean, bad


# ── WRITE FUNCTIONS ────────────────────────────────────────────────────────────

def write_silver(df, path):
    (df.withColumn("_transformed_at", F.lit(RUN_ID))
       .write.mode("overwrite")
       .parquet(path))


def write_quarantine(df, path):
    if df.rdd.isEmpty():
        return
    (df.withColumn("_run_id", F.lit(RUN_ID))
       .write.mode("overwrite")
       .parquet(path))


# ── HIVE REGISTRATION ──────────────────────────────────────────────────────────

def register_hive_tables(spark):
    """
    Registers external Hive tables pointing to Silver GCS paths.
    Uses explicit schema DDL — no CTAS, no data reading or writing.
    Tables dropped at start of main() before Silver write to prevent
    managed table deletion from overwriting Silver data.
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS fdic_silver")

    spark.sql(f"""
        CREATE EXTERNAL TABLE fdic_silver.institutions (
            cert            INT,
            name            STRING,
            city            STRING,
            stname          STRING,
            stalp           STRING,
            asset           BIGINT,
            repdte          STRING,
            active          INT,
            instcat         INT,
            charterclass    STRING,
            _ingested_at    STRING,
            _source         STRING,
            _transformed_at STRING
        )
        STORED AS PARQUET
        LOCATION '{SILVER_BASE}/institutions/'
    """)

    spark.sql(f"""
        CREATE EXTERNAL TABLE fdic_silver.financials (
            cert            INT,
            repdte          STRING,
            asset           BIGINT,
            dep             BIGINT,
            lnlsnet         BIGINT,
            intinc          BIGINT,
            eintexp         BIGINT,
            netinc          BIGINT,
            rbct1           DOUBLE,
            nperfv          BIGINT,
            _ingested_at    STRING,
            _source         STRING,
            _transformed_at STRING
        )
        PARTITIONED BY (year INT, quarter INT)
        STORED AS PARQUET
        LOCATION '{SILVER_BASE}/financials/'
    """)

    spark.sql("MSCK REPAIR TABLE fdic_silver.financials")

    print("Hive tables registered:")
    spark.sql("SHOW TABLES IN fdic_silver").show()


# ── MAIN ───────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("FDIC Silver Transformation — Starting")
    print(f"Run ID: {RUN_ID}")
    print("=" * 60)

    # ── Step 0: Drop Hive tables FIRST ──────────────────────────────
    # CRITICAL: Must happen before writing Silver.
    # Previous runs may have created managed tables (not truly external).
    # DROP TABLE on a managed table deletes the underlying GCS data.
    # Dropping BEFORE the Silver write ensures our fresh write is safe.
    print("\nDropping existing Hive tables (if any)...")
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS fdic_silver")
        spark.sql("DROP TABLE IF EXISTS fdic_silver.institutions")
        spark.sql("DROP TABLE IF EXISTS fdic_silver.financials")
        print("  Done")
    except Exception as e:
        print(f"  Warning (non-fatal): {e}")

    # ── Step 1: Institutions ─────────────────────────────────────────
    print("\nProcessing institutions...")
    inst = spark.read.parquet(f"{BRONZE_BASE}/institutions/")
    inst_clean, inst_bad = apply_institution_dq(inst)
    write_silver(inst_clean, f"{SILVER_BASE}/institutions/")
    write_quarantine(inst_bad, f"{QUARANTINE_BASE}/institutions/")
    print(f"  Total: {inst.count()} | Clean: {inst_clean.count()} | Quarantined: {inst_bad.count()}")

    # ── Step 2: Financials ───────────────────────────────────────────
    print("\nProcessing financials...")
    fin = spark.read.parquet(f"{BRONZE_BASE}/financials/")
    fin_clean, fin_bad = apply_financials_dq(fin)
    (fin_clean
        .withColumn("_transformed_at", F.lit(RUN_ID))
        .write.mode("overwrite")
        .partitionBy("year", "quarter")
        .parquet(f"{SILVER_BASE}/financials/"))
    write_quarantine(fin_bad, f"{QUARANTINE_BASE}/financials/")
    print(f"  Total: {fin.count()} | Clean: {fin_clean.count()} | Quarantined: {fin_bad.count()}")

    # ── Step 3: Register Hive tables ─────────────────────────────────
    print("\nRegistering Hive tables...")
    register_hive_tables(spark)

    print("\n" + "=" * 60)
    print("FDIC Silver Transformation — Completed Successfully")
    print(f"Verify: gcloud storage ls --recursive gs://{BUCKET}/silver/")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
