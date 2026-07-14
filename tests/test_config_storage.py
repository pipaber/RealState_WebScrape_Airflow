from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from urbania_scraper.config import ScrapeConfig
from urbania_scraper.storage import BronzeWriter


class ScrapeConfigTests(unittest.TestCase):
    def test_accepts_supported_bedroom_segments(self) -> None:
        for bedrooms in (1, 2, 3, 4):
            with self.subTest(bedrooms=bedrooms):
                self.assertEqual(ScrapeConfig(bedrooms=bedrooms).bedrooms, bedrooms)

    def test_rejects_unsupported_bedroom_segments(self) -> None:
        for bedrooms in (0, 5):
            with self.subTest(bedrooms=bedrooms):
                with self.assertRaises(ValueError):
                    ScrapeConfig(bedrooms=bedrooms)


class BronzeWriterTests(unittest.TestCase):
    def test_manifest_matches_the_data_product_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            cfg = ScrapeConfig(bedrooms=3, output_root=output_root)
            writer = BronzeWriter(cfg, run_id="scheduled_20260713_b3", ingest_date="2026-07-13")
            writer.open()
            writer.close(
                pages_scraped=2,
                started_at="2026-07-13T05:00:00+00:00",
                status="success",
            )

            manifest = json.loads(writer.manifest_path.read_text(encoding="utf-8"))

            self.assertEqual(manifest["run_id"], "scheduled_20260713_b3")
            self.assertEqual(manifest["status"], "success")
            self.assertEqual(manifest["records_written"], 0)
            self.assertEqual(manifest["pages_scraped"], 2)
            self.assertEqual(manifest["search_params"]["bedrooms"], 3)
            self.assertIsNone(manifest["error"])
            self.assertIn("completed_at", manifest)
            self.assertEqual(
                manifest["data_path"],
                "source=urbania/operation=alquiler/property=departamento/"
                "bedrooms=3/ingest_date=2026-07-13/"
                "listings_scheduled_20260713_b3.jsonl",
            )


if __name__ == "__main__":
    unittest.main()
