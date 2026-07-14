"""Bronze output writer: NDJSON in a Hive-style partition layout + a manifest.

Layout (maps 1:1 to an Azure Blob path so Airflow can upload it unchanged):

    data/bronze/source=urbania/operation=alquiler/property=departamento/
        bedrooms=<1|2|3|4>/ingest_date=YYYY-MM-DD/listings_<run_id>.jsonl

Each run writes one data file keyed by ``run_id`` (a new run = a new file). The
file is opened in truncate mode, so re-executing a run (same ``run_id``) rewrites
it cleanly rather than appending a duplicate pass.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .config import ScrapeConfig
from .models import BronzeListing


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_run_id() -> str:
    """Timestamp-based run id, e.g. 20260603T142530Z."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


class BronzeWriter:
    """Resolves the partition path and streams records to a single NDJSON file."""

    def __init__(self, cfg: ScrapeConfig, run_id: str, ingest_date: str | None = None):
        self.cfg = cfg
        self.run_id = run_id
        self.ingest_date = ingest_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.partition_dir = self._partition_dir()
        self.data_path = self.partition_dir / f"listings_{run_id}.jsonl"
        # Spark and Auto Loader ignore files whose names begin with ``_``.
        # Keep manifests visible to the Bronze ingestion source.
        self.manifest_path = self.partition_dir / f"manifest_{run_id}.json"
        self._count = 0

    def _partition_dir(self) -> Path:
        # Singular partition keys (operation/property) match common lake conventions.
        property_singular = self.cfg.property_type.rstrip("s")
        return (
            self.cfg.output_root
            / "source=urbania"
            / f"operation={self.cfg.operation}"
            / f"property={property_singular}"
            / f"bedrooms={self.cfg.bedrooms}"
            / f"ingest_date={self.ingest_date}"
        )

    def open(self) -> "BronzeWriter":
        self.partition_dir.mkdir(parents=True, exist_ok=True)
        # Truncate ("w"), not append: if the task is retried or double-dispatched
        # with the same run_id, the file is rewritten cleanly instead of having a
        # second pass appended (which would duplicate every listing).
        self._fh = self.data_path.open("w", encoding="utf-8")
        return self

    def write(self, listing: BronzeListing) -> None:
        self._fh.write(json.dumps(listing.to_jsonl_dict(), ensure_ascii=False) + "\n")
        self._count += 1

    @property
    def count(self) -> int:
        return self._count

    def close(
        self,
        *,
        pages_scraped: int,
        started_at: str,
        status: str = "success",
        error: str | None = None,
    ) -> None:
        self._fh.close()
        completed_at = utc_now_iso()
        relative_data_path = self.data_path.relative_to(self.cfg.output_root).as_posix()
        manifest = {
            "run_id": self.run_id,
            "status": status,
            "records_written": self._count,
            "pages_scraped": pages_scraped,
            "started_at": started_at,
            "completed_at": completed_at,
            "search_params": self.cfg.search_params(),
            "data_file": self.data_path.name,
            "data_path": relative_data_path,
            "partition": self.partition_dir.relative_to(self.cfg.output_root).as_posix(),
            "error": error,
            # Backwards-compatible aliases for manifests already consumed by
            # notebooks written before the Data Product contract was defined.
            "record_count": self._count,
            "finished_at": completed_at,
        }
        self.manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
