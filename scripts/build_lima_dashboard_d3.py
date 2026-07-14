"""Build the standalone D3.js version of the Lima rental dashboard."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

from plot_lima_bubble_map import (
    BEDROOM_COLORS,
    RENT_COLORS,
    load_boundaries,
    load_market_data,
    rent_bracket_labels,
    validate_scrape_date,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the D3.js Lima rental dashboard.")
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("--boundaries", type=Path, required=True)
    parser.add_argument("--district-column")
    parser.add_argument("--scrape-date")
    parser.add_argument(
        "--bins",
        nargs="+",
        type=float,
        default=[1500, 2000, 2500, 3000, 4000],
    )
    parser.add_argument(
        "--template",
        type=Path,
        default=Path("visualizations/lima_rental_dashboard_d3.template.html"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/lima_rental_dashboard_d3.html"),
    )
    return parser.parse_args()


def frame_records(frame: pd.DataFrame | None) -> list[dict]:
    if frame is None:
        return []
    return json.loads(frame.to_json(orient="records", force_ascii=False))


def main() -> int:
    args = parse_args()
    try:
        if args.bins != sorted(set(args.bins)):
            raise ValueError("--bins must contain unique values in ascending order.")
        market, detected_date, bedroom_market = load_market_data(args.input_csv, pd)
        scrape_date = validate_scrape_date(args.scrape_date or detected_date)
        boundaries = load_boundaries(args.boundaries, args.district_column, gpd)

        # The source export can still contain neighbourhood labels. Keep only
        # records that join to an official Lima/Callao district so the D3 totals
        # and rankings describe exactly the same geography as the map.
        boundary_keys = set(boundaries["district_key"])
        market = market.loc[market["district_key"].isin(boundary_keys)].copy()
        if market.empty:
            raise ValueError("No Gold district matched a district in the Shapefile.")
        if bedroom_market is not None:
            bedroom_market = bedroom_market.loc[
                bedroom_market["district_key"].isin(boundary_keys)
            ].copy()

        simplified = boundaries.to_crs(epsg=3857).copy()
        simplified.geometry = simplified.geometry.simplify(90, preserve_topology=True)
        simplified = simplified.to_crs(epsg=4326)

        payload = {
            "scrapeDate": scrape_date,
            "bins": args.bins,
            "binLabels": rent_bracket_labels(args.bins),
            "rentColors": list(RENT_COLORS),
            "bedroomColors": {str(key): value for key, value in BEDROOM_COLORS.items()},
            "geojson": json.loads(simplified.to_json(drop_id=True)),
            "market": frame_records(market),
            "bedroomMarket": frame_records(bedroom_market),
        }
        template_path = args.template
        if not template_path.exists() and args.output.exists():
            # The generated dashboard is also a valid reusable shell. This
            # keeps regeneration possible when only the standalone deliverable
            # was retained and the original template was removed.
            template_path = args.output
        template = template_path.read_text(encoding="utf-8")
        marker = "__DASHBOARD_DATA__"
        serialized_payload = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":")
        )
        if marker in template:
            html = template.replace(marker, serialized_payload)
        else:
            html, replacements = re.subn(
                r"(?<=    const data = ).*?(?=;\r?\n    const market = data\.market;)",
                serialized_payload,
                template,
                count=1,
                flags=re.DOTALL,
            )
            if replacements != 1:
                raise ValueError(
                    f"Template is missing {marker} and has no replaceable embedded payload."
                )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(html, encoding="utf-8")
    except (OSError, RuntimeError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    print(f"Created: {args.output.resolve()}")
    print(f"Scrape date: {scrape_date}")
    print(f"Mapped: {market['listing_count'].sum():,.0f} listings in {len(market)} districts")
    print(f"Bedroom dimension: {'available' if bedroom_market is not None else 'missing'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
