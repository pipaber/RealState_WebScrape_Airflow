"""Create four shared-scale value matrices from the governed Gold export."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from plot_lima_bubble_map import (
    DISTRICT_ALIASES,
    REQUIRED_COLUMNS,
    load_boundaries,
    normalize,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare average area, median rent and supply for 1–4 bedrooms."
    )
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("--boundaries", type=Path, required=True)
    parser.add_argument("--district-column")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/lima_rental_value_matrix.png"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        import geopandas as gpd
        import matplotlib.pyplot as plt
        import numpy as np
        import pandas as pd
        from matplotlib.colors import Normalize
        from matplotlib.ticker import FuncFormatter, NullFormatter

        data = pd.read_csv(args.input_csv)
        missing = REQUIRED_COLUMNS - set(data.columns)
        if missing:
            raise ValueError(
                "Gold export CSV missing columns: " + ", ".join(sorted(missing))
            )
        dates = sorted(data["ingest_date"].dropna().astype(str).str[:10].unique())
        if len(dates) != 1:
            raise ValueError("The Gold export must contain exactly one ingest_date.")

        numeric = [
            "bedrooms",
            "listing_count",
            "median_price",
            "avg_area_m2",
            "median_price_per_m2",
        ]
        for column in numeric:
            data[column] = pd.to_numeric(data[column], errors="coerce")
        data = data.loc[
            (data["currency"].astype(str).str.upper() == "PEN")
            & data["bedrooms"].between(1, 4)
            & (data["listing_count"] > 0)
        ].dropna(subset=numeric + ["district"])
        data["bedrooms"] = data["bedrooms"].astype(int)
        data["district_key"] = data["district"].map(normalize).replace(DISTRICT_ALIASES)

        boundaries = load_boundaries(
            args.boundaries, args.district_column, gpd
        )
        official_keys = set(boundaries["district_key"])
        data = data.loc[data["district_key"].isin(official_keys)].copy()
        if data.empty:
            raise ValueError("No Gold district matched the official Shapefile.")

        x_min, x_max = data["avg_area_m2"].min(), data["avg_area_m2"].max()
        y_min, y_max = data["median_price"].min(), data["median_price"].max()
        x_pad = max((x_max - x_min) * 0.08, 4)
        y_pad = max((y_max - y_min) * 0.08, 150)
        colour_norm = Normalize(
            vmin=data["median_price_per_m2"].min(),
            vmax=data["median_price_per_m2"].max(),
        )

        figure, axes = plt.subplots(
            2, 2, figsize=(16, 11), sharex=True, sharey=True, constrained_layout=False
        )
        figure.patch.set_facecolor("white")
        scatter = None
        for bedrooms, axis in zip(range(1, 5), axes.flat):
            subset = data.loc[data["bedrooms"] == bedrooms].copy()
            axis.set_facecolor("#f8fafc")
            scatter = axis.scatter(
                subset["avg_area_m2"],
                subset["median_price"],
                s=36 + np.sqrt(subset["listing_count"]) * 28,
                c=subset["median_price_per_m2"],
                cmap="viridis",
                norm=colour_norm,
                edgecolors="#334155",
                linewidths=0.7,
                alpha=0.88,
            )
            area_median = np.average(
                subset["avg_area_m2"], weights=subset["listing_count"]
            )
            rent_median = np.average(
                subset["median_price"], weights=subset["listing_count"]
            )
            axis.axvline(area_median, color="#94a3b8", linewidth=1, linestyle="--")
            axis.axhline(rent_median, color="#94a3b8", linewidth=1, linestyle="--")
            label_rows = pd.concat(
                [
                    subset.nlargest(3, "listing_count"),
                    subset.nlargest(1, "median_price"),
                ]
            ).drop_duplicates(subset=["district_key"])
            offsets = ((6, 6), (6, -12), (-6, 7), (-6, -12))
            for ((_, row), offset) in zip(label_rows.iterrows(), offsets):
                axis.annotate(
                    row["district"],
                    (row["avg_area_m2"], row["median_price"]),
                    xytext=offset,
                    textcoords="offset points",
                    fontsize=8,
                    color="#1f2937",
                    ha="right" if offset[0] < 0 else "left",
                )
            axis.set_title(
                f"{bedrooms} cuarto{'s' if bedrooms > 1 else ''} · "
                f"{int(subset['listing_count'].sum()):,} avisos",
                loc="left",
                fontsize=12,
                fontweight="bold",
            )
            axis.set_xscale("log")
            axis.set_yscale("log")
            axis.set_xlim(max(1, x_min * 0.78), x_max * 1.12)
            axis.set_ylim(max(100, y_min * 0.72), y_max * 1.18)
            axis.set_xticks(
                [value for value in (25, 40, 60, 80, 120, 180, 260, 340) if x_min - x_pad <= value <= x_max + x_pad]
            )
            axis.set_yticks(
                [value for value in (500, 1000, 2000, 4000, 8000, 12000) if y_min - y_pad <= value <= y_max + y_pad]
            )
            axis.xaxis.set_major_formatter(
                FuncFormatter(lambda value, _position: f"{value:,.0f}")
            )
            axis.xaxis.set_minor_formatter(NullFormatter())
            axis.yaxis.set_minor_formatter(NullFormatter())
            axis.grid(color="#e2e8f0", linewidth=0.7)
            axis.set_axisbelow(True)
            axis.spines[["top", "right"]].set_visible(False)
            axis.spines[["bottom", "left"]].set_color("#cbd5e1")
            axis.yaxis.set_major_formatter(
                FuncFormatter(lambda value, _position: f"S/ {value / 1000:.1f}k")
            )

        for axis in axes[-1, :]:
            axis.set_xlabel("Área promedio (m², escala log)")
        for axis in axes[:, 0]:
            axis.set_ylabel("Alquiler mediano mensual (PEN, escala log)")

        colour_axis = figure.add_axes([0.905, 0.25, 0.016, 0.52])
        colour_bar = figure.colorbar(scatter, cax=colour_axis)
        colour_bar.set_label("Precio mediano por m² (PEN)")
        size_handles = [
            plt.scatter([], [], s=36 + np.sqrt(value) * 28, color="#94a3b8", edgecolor="#334155")
            for value in (5, 20, 50)
        ]
        figure.legend(
            size_handles,
            ["5 avisos", "20 avisos", "50 avisos"],
            title="Tamaño de burbuja",
            loc="lower center",
            ncol=3,
            frameon=False,
            bbox_to_anchor=(0.48, 0.025),
        )
        figure.suptitle(
            "Relación entre espacio, alquiler y profundidad de mercado",
            fontsize=20,
            fontweight="bold",
            x=0.055,
            y=0.975,
            ha="left",
        )
        figure.text(
            0.055,
            0.935,
            f"Scrape: {dates[0]} · Cada punto es un distrito · Color = precio por m² · Burbuja = cantidad de avisos",
            fontsize=10.5,
            color="#475569",
        )
        figure.text(
            0.055,
            0.015,
            "Fuente exclusiva: Gold market_daily_by_district · Moneda PEN · Límites distritales: IGN/IDEP",
            fontsize=8.5,
            color="#64748b",
        )
        figure.subplots_adjust(
            left=0.07, right=0.87, bottom=0.11, top=0.89, wspace=0.15, hspace=0.20
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        figure.savefig(args.output, dpi=220, bbox_inches="tight", facecolor="white")
        plt.close(figure)
    except (OSError, RuntimeError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    print(f"Created: {args.output.resolve()}")
    print(f"Gold rows plotted: {len(data)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
