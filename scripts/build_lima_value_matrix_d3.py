"""Build the standalone D3.js value matrix from the governed Gold export."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

from plot_lima_bubble_map import (
    DISTRICT_ALIASES,
    REQUIRED_COLUMNS,
    load_boundaries,
    normalize,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the D3.js Lima rental value matrix.")
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("--boundaries", type=Path, required=True)
    parser.add_argument("--district-column")
    parser.add_argument(
        "--template",
        type=Path,
        default=Path("visualizations/lima_rental_value_matrix_d3.template.html"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/lima_rental_value_matrix_d3.html"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        data = pd.read_csv(args.input_csv)
        missing = REQUIRED_COLUMNS - set(data.columns)
        if missing:
            raise ValueError(
                "Gold export CSV missing columns: " + ", ".join(sorted(missing))
            )
        dates = sorted(data["ingest_date"].dropna().astype(str).str[:10].unique())
        if len(dates) != 1:
            raise ValueError("The Gold export must contain exactly one ingest_date.")

        metrics = [
            "bedrooms",
            "listing_count",
            "median_price",
            "avg_area_m2",
            "median_price_per_m2",
        ]
        for column in metrics:
            data[column] = pd.to_numeric(data[column], errors="coerce")
        data = data.loc[
            (data["currency"].astype(str).str.upper() == "PEN")
            & data["bedrooms"].between(1, 4)
            & (data["listing_count"] > 0)
        ].dropna(subset=metrics + ["district"])
        data["bedrooms"] = data["bedrooms"].astype(int)
        data["district_key"] = data["district"].map(normalize).replace(DISTRICT_ALIASES)

        boundaries = load_boundaries(args.boundaries, args.district_column, gpd)
        official_keys = set(boundaries["district_key"])
        data = data.loc[data["district_key"].isin(official_keys)].copy()
        if data.empty:
            raise ValueError("No Gold district matched the official Shapefile.")

        records = data[
            [
                "district",
                "district_key",
                "bedrooms",
                "listing_count",
                "median_price",
                "avg_area_m2",
                "median_price_per_m2",
            ]
        ].round(2)
        payload = {
            "scrapeDate": dates[0],
            "rows": json.loads(records.to_json(orient="records", force_ascii=False)),
        }
        template = args.template.read_text(encoding="utf-8")
        marker = "__VALUE_MATRIX_DATA__"
        if marker not in template:
            raise ValueError(f"Template is missing {marker}.")
        html = template.replace(
            marker,
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(html, encoding="utf-8")
    except (OSError, RuntimeError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    print(f"Created: {args.output.resolve()}")
    print(f"Scrape date: {dates[0]}")
    print(f"Gold rows plotted: {len(records)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
