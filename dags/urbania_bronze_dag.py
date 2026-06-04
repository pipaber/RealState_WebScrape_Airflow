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

from airflow.sdk import dag, task
from pendulum import timezone

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


@dag(
    dag_id="urbania_bronze",
    description="Scrape urbania.pe 2-bed Lima rentals and land them in Azure bronze.",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 6, 1, tzinfo=timezone("America/Lima")),
    catchup=False,
    max_active_runs=1,  # no overlapping runs writing the same partition
    tags=["urbania", "bronze", "scraping"],
)
def urbania_bronze():
    """Scrape Urbania and upload the bronze partition to Azure."""

    @task
    def scrape_urbania() -> str:
        """Run the scraper and return the bronze partition directory."""
        import asyncio
        import sys

        sys.path.insert(0, SCRAPER_SRC)
        from urbania_scraper.config import ScrapeConfig
        from urbania_scraper.scraper import scrape

        cfg = ScrapeConfig(
            operation="alquiler",
            property_type="departamentos",
            location="lima",
            bedrooms=2,
            max_pages=0,  # all pages
            headless=True,
            output_root=RAW_ROOT,
        )
        summary = asyncio.run(scrape(cfg))
        if summary["status"] != "success":
            raise RuntimeError(f"scrape failed: {summary['status']}")
        # The data file's parent is the partition dir we want to upload.
        return str(Path(summary["output"]).parent)

    @task
    def upload_to_azure(partition_dir: str) -> None:
        """Upload the run's partition to Azure Blob Storage."""
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

        partition_dir = Path(partition_dir)
        hook = WasbHook(wasb_conn_id=AZURE_CONN_ID)
        # Ensure the target container exists (idempotent: no-op if already present).
        hook.create_container(AZURE_CONTAINER)
        uploaded = 0
        for f in partition_dir.rglob("*"):
            if f.is_file():
                # Preserve the lake layout under the target container.
                blob_name = str(f.relative_to(RAW_ROOT))
                hook.load_file(
                    file_path=str(f),
                    container_name=AZURE_CONTAINER,
                    blob_name=blob_name,
                    overwrite=True,
                )
                uploaded += 1

        print(f"Uploaded {uploaded} files to Azure container '{AZURE_CONTAINER}'.")

    upload_to_azure(scrape_urbania())


dag = urbania_bronze()
