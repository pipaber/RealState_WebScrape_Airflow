"""Airflow DAG: scrape urbania.pe -> upload output to Azure Blob Storage.

The two task types are dynamically mapped for bedrooms 1, 2, 3, and 4:

    scrape_urbania[bedrooms]  ->  upload_to_azure[run artifacts]

The upload needs an Azure connection (``AZURE_CONN_ID``) configured in Airflow
(Admin -> Connections); no secrets live in source. The project Dockerfile
installs the deps, Playwright's Chromium, and the scraper source at SCRAPER_SRC.

This file is not imported by the scraper; it's deployed into Airflow's dags/.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task
from pendulum import timezone

# --- Configuration (override via Airflow Variables / env) ---------------------
# Mounted Airflow data volume root inside the container.
DATA_DIR = Path(os.environ.get("URBANIA_PROJECT_DIR", "/opt/airflow/data"))
RAW_ROOT = DATA_DIR / "raw" / "airflow" / "G1"

# Where the urbania_scraper source lives in the image (Dockerfile: COPY src ./src).
# Kept separate from DATA_DIR, which points at the mounted data volume.
SCRAPER_SRC = os.environ.get("URBANIA_SRC", "/opt/airflow/src")

AZURE_CONN_ID = "utec_blob_storage"   # configure in Airflow Connections
AZURE_CONTAINER = "datalake"
BEDROOM_SEGMENTS = [1, 2, 3, 4]
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="urbania_bronze",
    description="Scrape urbania.pe 1-4 bedroom Lima rentals and land them in Azure raw.",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 6, 1, tzinfo=timezone("America/Lima")),
    catchup=False,
    max_active_runs=1,  # no overlapping runs writing the same partition
    max_active_tasks=1,  # be polite to the source: scrape one segment at a time
    tags=["urbania", "bronze", "scraping"],
)
def urbania_bronze():
    """Scrape Urbania and upload the bronze partition to Azure."""

    @task
    def scrape_urbania(bedrooms: int) -> dict[str, str | int]:
        """Run one bedroom segment and return only this run's artifacts."""
        import asyncio
        import sys

        from airflow.sdk import get_current_context

        context = get_current_context()
        airflow_run_id = str(context["ti"].run_id)
        safe_airflow_run_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", airflow_run_id).strip("_")
        stable_run_id = f"{safe_airflow_run_id}_b{bedrooms}"

        logger.info("Starting Urbania scrape task for bedrooms=%s.", bedrooms)
        logger.info("Using scraper source path: %s", SCRAPER_SRC)
        logger.info("Writing raw output under: %s", RAW_ROOT)

        sys.path.insert(0, SCRAPER_SRC)
        from urbania_scraper.config import ScrapeConfig
        from urbania_scraper.scraper import scrape

        cfg = ScrapeConfig(
            operation="alquiler",
            property_type="departamentos",
            location="lima",
            bedrooms=bedrooms,
            run_id=stable_run_id,
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
        logger.info("Scrape output file: %s", summary["output"])
        logger.info("Scrape manifest file: %s", summary["manifest"])
        return {
            "bedrooms": bedrooms,
            "run_id": summary["run_id"],
            "data_file": summary["output"],
            "manifest_file": summary["manifest"],
        }

    @task
    def upload_to_azure(run_artifacts: dict[str, str | int]) -> None:
        """Upload only the immutable data and manifest files from one run."""
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

        artifacts = [
            Path(str(run_artifacts["data_file"])),
            Path(str(run_artifacts["manifest_file"])),
        ]
        logger.info("Starting Azure upload task.")
        logger.info(
            "Run received: run_id=%s bedrooms=%s",
            run_artifacts["run_id"],
            run_artifacts["bedrooms"],
        )
        logger.info("Azure container: %s", AZURE_CONTAINER)
        logger.info("Azure connection id: %s", AZURE_CONN_ID)

        hook = WasbHook(wasb_conn_id=AZURE_CONN_ID)
        # Ensure the target container exists (idempotent: no-op if already present).
        logger.info("Ensuring Azure container exists: %s", AZURE_CONTAINER)
        hook.create_container(AZURE_CONTAINER)
        uploaded = 0
        skipped = 0
        for artifact in artifacts:
            if not artifact.is_file():
                raise FileNotFoundError(f"run artifact does not exist: {artifact}")
            # Preserve the full lake path: raw/airflow/G1/... under the container.
            # Legacy manifests started with '_' and are invisible to Spark's file
            # sources. Rename only their blob key during republishing; the local
            # artifact remains immutable and the new key is idempotent.
            blob_name = artifact.relative_to(DATA_DIR).as_posix()
            if artifact.name.startswith("_manifest_"):
                blob_name = blob_name.replace("/_manifest_", "/manifest_", 1)
            if hook.check_for_blob(AZURE_CONTAINER, blob_name):
                logger.info("Immutable blob already exists; skipping: %s", blob_name)
                skipped += 1
                continue
            logger.info("Uploading file to Azure Blob Storage: %s -> %s", artifact, blob_name)
            hook.load_file(
                file_path=str(artifact),
                container_name=AZURE_CONTAINER,
                blob_name=blob_name,
                overwrite=False,
            )
            uploaded += 1

        logger.info("Uploaded %s files to Azure container '%s'.", uploaded, AZURE_CONTAINER)
        logger.info("Skipped %s immutable blobs that already existed.", skipped)

    scrape_results = scrape_urbania.expand(bedrooms=BEDROOM_SEGMENTS)
    upload_to_azure.expand(run_artifacts=scrape_results)


dag = urbania_bronze()
