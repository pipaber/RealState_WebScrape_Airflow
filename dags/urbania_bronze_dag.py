"""Airflow DAG: scrape urbania.pe -> upload output to Azure Blob Storage.

Two tasks:

    scrape_urbania  ->  upload_to_azure

The upload needs an Azure connection (``AZURE_CONN_ID``) configured in Airflow
(Admin -> Connections); no secrets live in source. The project Dockerfile
installs the deps, Playwright's Chromium, and the scraper source at SCRAPER_SRC.

This file is not imported by the scraper; it's deployed into Airflow's dags/.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# --- Configuration (override via Airflow Variables / env) ---------------------
# Where the scraper writes output (the data volume).
PROJECT_DIR = os.environ.get("URBANIA_PROJECT_DIR", "/opt/airflow/data/g1_data")
RAW_ROOT = Path(PROJECT_DIR) / "raw" / "G1"

# Where the urbania_scraper source lives in the image (Dockerfile: COPY src ./src).
# Kept separate from PROJECT_DIR, which points at the mounted data volume.
SCRAPER_SRC = os.environ.get("URBANIA_SRC", "/opt/airflow/src")

AZURE_CONN_ID = "azure_blob_storage"   # configure in Airflow Connections
AZURE_CONTAINER = "airflow"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _run_scraper(**context) -> str:
    """Run the scraper for 2-bed Lima rentals; return the bronze partition dir.

    Returns the directory via XCom so the upload task knows what to push.
    """
    import sys

    sys.path.insert(0, SCRAPER_SRC)
    from urbania_scraper.config import ScrapeConfig
    from urbania_scraper.scraper import scrape
    import asyncio

    cfg = ScrapeConfig(
        operation="alquiler",
        property_type="departamentos",
        location="lima",
        bedrooms=2,
        max_pages=0,           # all pages
        headless=True,
        output_root=RAW_ROOT,
    )
    summary = asyncio.run(scrape(cfg))
    if summary["status"] != "success":
        raise RuntimeError(f"scrape failed: {summary['status']}")
    # The data file's parent is the partition dir we want to upload.
    return str(Path(summary["output"]).parent)


def _upload_to_azure(**context) -> None:
    """Upload the run's partition to Azure Blob Storage, mirroring the lake layout.

    Requires an Airflow connection named by ``AZURE_CONN_ID`` (type: Azure Blob
    Storage) configured with the storage account name + key / SAS / managed
    identity. No credentials are kept in code.
    """
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    partition_dir = Path(context["ti"].xcom_pull(task_ids="scrape_urbania"))
    hook = WasbHook(wasb_conn_id=AZURE_CONN_ID)
    # Ensure the target container exists (idempotent: no-op if already present).
    hook.create_container(AZURE_CONTAINER)
    uploaded = 0
    for f in partition_dir.rglob("*"):
        if f.is_file():
            # Keep the partition path under the container so the lake layout
            # (source=.../operation=.../.../ingest_date=.../*.jsonl) is preserved.
            blob_name = str(f.relative_to(RAW_ROOT))
            hook.load_file(
                file_path=str(f),
                container_name=AZURE_CONTAINER,
                blob_name=blob_name,
                overwrite=True,
            )
            uploaded += 1

    print(f"Uploaded {uploaded} files to Azure container '{AZURE_CONTAINER}'.")


with DAG(
    dag_id="urbania_bronze",
    description="Scrape urbania.pe 2-bed Lima rentals and land them in Azure bronze.",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,  # no overlapping runs writing the same partition
    tags=["urbania", "bronze", "scraping"],
) as dag:
    scrape_urbania = PythonOperator(
        task_id="scrape_urbania",
        python_callable=_run_scraper,
    )
    upload_to_azure = PythonOperator(
        task_id="upload_to_azure",
        python_callable=_upload_to_azure,
    )

    scrape_urbania >> upload_to_azure
