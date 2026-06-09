"""
fdic_bronze_ingest.py

Purpose: Generate realistic FDIC-like banking data and write to GCS Bronze layer
         as Parquet files partitioned by year/quarter.

NOTE ON DATA SOURCE:
  The FDIC public API blocks requests from GCP IP ranges (Cloud Shell / Dataproc).
  This script generates realistic synthetic banking data matching the FDIC schema.
  The pipeline architecture, GCS structure, Parquet format, partitioning strategy,
  idempotency logic, and DQ framework are identical to production.

  Interview framing: "The pipeline is architected for FDIC regulatory data ingestion.
  In the sandbox environment we use synthetic data matching the FDIC schema because
  the public API restricts access from cloud provider IP ranges."
"""

import pandas as pd
import numpy as np
import io
import json
import logging
from datetime import datetime
from google.cloud import storage

# ── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ID  = "gp-ct-sbox-con-gcp07f-de"
BUCKET_NAME = "fdic-banking-pipeline-swagath"
RANDOM_SEED = 42

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def get_gcs_client():
    return storage.Client(project=PROJECT_ID)


def read_checkpoint(client):
    bucket = client.bucket(BUCKET_NAME)
    blob   = bucket.blob("checkpoints/run_manifest.json")
    try:
        return json.loads(blob.download_as_text())
    except Exception:
        logger.info("No checkpoint manifest found — starting fresh run")
        return {}


def write_checkpoint(client, manifest):
    bucket = client.bucket(BUCKET_NAME)
    blob   = bucket.blob("checkpoints/run_manifest.json")
    blob.upload_from_string(json.dumps(manifest, indent=2))
    logger.info("Checkpoint manifest updated in GCS")


def write_parquet_to_gcs(client, df, gcs_path):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
    buffer.seek(0)
    size_kb = buffer.getbuffer().nbytes / 1024
    bucket = client.bucket(BUCKET_NAME)
    blob   = bucket.blob(gcs_path)
    blob.upload_from_file(buffer, content_type="application/octet-stream")
    logger.info(f"Written {len(df):,} records ({size_kb:.1f} KB) to gs://{BUCKET_NAME}/{gcs_path}")
    return len(df)


def generate_institutions(n=500):
    rng = np.random.default_rng(RANDOM_SEED)
    prefixes  = ['First','United','Heritage','Summit','Pacific','Central',
                 'Community','National','American','Midwest','Coastal','Pioneer']
    suffixes  = ['Bank','Savings Bank','Financial','Trust','Bancorp','Federal Savings']
    states    = [('California','CA'),('Texas','TX'),('New York','NY'),
                 ('Florida','FL'),('Illinois','IL'),('Pennsylvania','PA'),
                 ('Ohio','OH'),('Georgia','GA'),('North Carolina','NC'),
                 ('Michigan','MI'),('Virginia','VA'),('Arizona','AZ'),
                 ('Washington','WA'),('Colorado','CO'),('Tennessee','TN')]
    cities    = ['New York','Los Angeles','Chicago','Houston','Phoenix',
                 'Dallas','Austin','Jacksonville','Columbus','Denver',
                 'Seattle','Nashville','Atlanta','Miami','Charlotte']
    state_pairs = [states[i % len(states)] for i in range(n)]
    df = pd.DataFrame({
        'cert':         range(10001, 10001 + n),
        'name':         [f"{prefixes[i % len(prefixes)]} {suffixes[i % len(suffixes)]}" for i in range(n)],
        'city':         [cities[i % len(cities)] for i in range(n)],
        'stname':       [s[0] for s in state_pairs],
        'stalp':        [s[1] for s in state_pairs],
        'asset':        rng.integers(50_000, 50_000_000, n),
        'repdte':       '20241231',
        'active':       rng.choice([1,1,1,1,1,1,1,1,1,1,1,0], n),
        'instcat':      rng.integers(1, 5, n),
        'charterclass': rng.choice(['N','SM','NM','SB'], n),
    })
    return df


def generate_financials(year, quarter, seed_offset=0, n=500):
    rng      = np.random.default_rng(RANDOM_SEED + seed_offset)
    qend     = {1:f"{year}0331",2:f"{year}0630",3:f"{year}0930",4:f"{year}1231"}
    assets   = rng.integers(50_000, 50_000_000, n)
    deposits = (assets * rng.uniform(0.65, 0.85, n)).astype(int)
    loans    = (assets * rng.uniform(0.45, 0.70, n)).astype(int)
    df = pd.DataFrame({
        'cert':    range(10001, 10001 + n),
        'repdte':  qend[quarter],
        'asset':   assets,
        'dep':     deposits,
        'lnlsnet': loans,
        'intinc':  (assets * rng.uniform(0.01,  0.015, n)).astype(int),
        'eintexp': (assets * rng.uniform(0.002, 0.006, n)).astype(int),
        'netinc':  (assets * rng.uniform(0.001, 0.004, n)).astype(int),
        'rbct1':   np.round(rng.uniform(8.0, 18.0, n), 2),
        'nperfv':  (assets * rng.uniform(0.005, 0.03, n)).astype(int),
        'year':    year,
        'quarter': quarter,
    })
    return df


def ingest_institutions(client, manifest):
    run_key = "institutions_master"
    if run_key in manifest:
        logger.info(f"SKIP {run_key} — completed at {manifest[run_key]}")
        return
    logger.info("Generating institutions master data")
    df = generate_institutions(n=500)
    df["_ingested_at"] = datetime.utcnow().isoformat()
    df["_source"]      = "fdic_synthetic_v1"
    count = write_parquet_to_gcs(client, df, "bronze/institutions/institutions.parquet")
    manifest[run_key] = datetime.utcnow().isoformat()
    logger.info(f"Institutions complete — {count:,} records")


def ingest_financials(client, manifest, year, quarter):
    run_key = f"financials_{year}_Q{quarter}"
    if run_key in manifest:
        logger.info(f"SKIP {run_key} — completed at {manifest[run_key]}")
        return
    logger.info(f"Generating financials for {year} Q{quarter}")
    df = generate_financials(year=year, quarter=quarter, seed_offset=year * 10 + quarter)
    df["_ingested_at"] = datetime.utcnow().isoformat()
    df["_source"]      = "fdic_synthetic_v1"
    gcs_path = f"bronze/financials/year={year}/quarter=Q{quarter}/data.parquet"
    count = write_parquet_to_gcs(client, df, gcs_path)
    manifest[run_key] = datetime.utcnow().isoformat()
    logger.info(f"Financials {year} Q{quarter} complete — {count:,} records")


def main():
    logger.info("=" * 60)
    logger.info("FDIC Bronze Ingestion Pipeline — Starting")
    logger.info("=" * 60)

    client   = get_gcs_client()
    manifest = read_checkpoint(client)

    ingest_institutions(client, manifest)

    for year, quarter in [
        (2023, 1), (2023, 2), (2023, 3), (2023, 4),
        (2024, 1), (2024, 2), (2024, 3), (2024, 4)
    ]:
        ingest_financials(client, manifest, year, quarter)

    write_checkpoint(client, manifest)

    logger.info("=" * 60)
    logger.info("FDIC Bronze Ingestion Pipeline — Completed Successfully")
    logger.info("=" * 60)
    logger.info(f"Verify: gsutil ls -r gs://{BUCKET_NAME}/bronze/")


if __name__ == "__main__":
    main()
