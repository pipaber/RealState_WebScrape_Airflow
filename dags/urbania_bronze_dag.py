"""Airflow DAG: scrape urbania.pe -> upload output to Azure Blob Storage.

Two tasks:

    scrape_urbania  ->  upload_to_azure

The upload needs an Azure connection (``AZURE_CONN_ID``) configured in Airflow
(Admin -> Connections); no secrets live in source. The project Dockerfile
installs the deps, Playwright's Chromium, and the scraper source at SCRAPER_SRC.

This file is not imported by the scraper; it's deployed into Airflow's dags/.
"""

from __future__ import annotations

import logging
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
logger = logging.getLogger(__name__)

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

        logger.info("Starting Urbania scrape task.")
        logger.info("Using scraper source path: %s", SCRAPER_SRC)
        logger.info("Writing raw output under: %s", RAW_ROOT)

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
        logger.info(
            "Scrape config: operation=%s, property_type=%s, location=%s, bedrooms=%s, max_pages=%s, headless=%s",
            cfg.operation,
            cfg.property_type,
            cfg.location,
            cfg.bedrooms,
            cfg.max_pages,
            cfg.headless,
        )

        summary = asyncio.run(scrape(cfg))
        logger.info("Scrape finished with status: %s", summary.get("status"))
        if summary["status"] != "success":
            logger.error("Scrape failed. Summary: %s", summary)
            raise RuntimeError(f"scrape failed: {summary['status']}")
        # The data file's parent is the partition dir we want to upload.
        partition_dir = Path(summary["output"]).parent
        logger.info("Scrape output file: %s", summary["output"])
        logger.info("Returning partition directory for upload: %s", partition_dir)
        return str(partition_dir)

    @task
    def upload_to_azure(partition_dir: str) -> None:
        """Upload the run's partition to Azure Blob Storage."""
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

        partition_dir = Path(partition_dir)
        logger.info("Starting Azure upload task.")
        logger.info("Partition directory received from scrape task: %s", partition_dir)
        logger.info("Azure container: %s", AZURE_CONTAINER)
        logger.info("Azure connection id: %s", AZURE_CONN_ID)

        if not partition_dir.exists():
            logger.error("Partition directory does not exist: %s", partition_dir)
            raise FileNotFoundError(f"partition directory does not exist: {partition_dir}")

        hook = WasbHook(wasb_conn_id=AZURE_CONN_ID)
        # Ensure the target container exists (idempotent: no-op if already present).
        logger.info("Ensuring Azure container exists: %s", AZURE_CONTAINER)
        hook.create_container(AZURE_CONTAINER)
        uploaded = 0
        for f in partition_dir.rglob("*"):
            if f.is_file():
                # Preserve the lake layout under the target container.
                blob_name = str(f.relative_to(RAW_ROOT))
                logger.debug("Uploading file to Azure Blob Storage: %s -> %s", f, blob_name)
                hook.load_file(
                    file_path=str(f),
                    container_name=AZURE_CONTAINER,
                    blob_name=blob_name,
                    overwrite=True,
                )
                uploaded += 1

        if uploaded == 0:
            logger.warning("No files found to upload from partition directory: %s", partition_dir)
        logger.info("Uploaded %s files to Azure container '%s'.", uploaded, AZURE_CONTAINER)

    upload_to_azure(scrape_urbania())


dag = urbania_bronze()
