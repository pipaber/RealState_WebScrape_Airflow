"""Plot a binned Lima rental choropleth from Gold data and an official Shapefile.

Example:
    uv run python scripts/plot_lima_bubble_map.py \
      "C:\\Users\\LENOVO\\Downloads\\Nuevo_notebook_2026_07_13_20_35_33_.csv" \
      --boundaries data/reference/limites_distritales.zip

The Shapefile can be the ZIP downloaded from the IGN/IDEP geoportal; it does
not need to be extracted. See docs/LIMA_BUBBLE_MAP.md for the data source.
"""

from __future__ import annotations

import argparse
from datetime import date
import re
import sys
import unicodedata
from pathlib import Path


REQUIRED_COLUMNS = {
    "ingest_date",
    "district",
    "bedrooms",
    "currency",
    "listing_count",
    "avg_price",
    "median_price",
    "min_price",
    "max_price",
    "avg_area_m2",
    "median_price_per_m2",
    "new_listing_count",
    "removed_listing_count",
    "price_changed_count",
    "offer_change_pct",
    "calculated_at",
}

DISTRICT_ALIASES = {
    "ATE VITARTE": "ATE",
    "LIMA CERCADO": "LIMA",
    "MAGDALENA": "MAGDALENA DEL MAR",
}

DISTRICT_COLUMN_CANDIDATES = (
    "NOMBDIST",
    "NOMB_DIST",
    "NOMBRE_DIST",
    "DISTRITO",
    "DISTRICT",
    "DIST_NAME",
)

DEPARTMENT_COLUMN_CANDIDATES = (
    "NOMBDEP",
    "NOMB_DPTO",
    "DEPARTAMENTO",
    "DEPARTAMEN",
    "DEPARTMENT",
)

PROVINCE_COLUMN_CANDIDATES = (
    "NOMBPROV",
    "NOMB_PROV",
    "PROVINCIA",
    "PROVINCE",
)

LIMA_METROPOLITANA_DEPARTMENTS = {
    "LIMA",
    "CALLAO",
    "PROVINCIA CONSTITUCIONAL DEL CALLAO",
}

LIMA_METROPOLITANA_PROVINCES = {
    "LIMA",
    "CALLAO",
    "PROVINCIA CONSTITUCIONAL DEL CALLAO",
}

# Sequential, colour-blind-friendly blues. The lightest band remains visible
# against the neutral no-data fill, unlike near-white yellow palettes.
RENT_COLORS = (
    "#deebf7",
    "#c6dbef",
    "#9ecae1",
    "#6baed6",
    "#3182bd",
    "#08519c",
)

BEDROOM_COLORS = {
    1: "#4c78a8",
    2: "#f58518",
    3: "#54a24b",
    4: "#e45756",
}


def normalize(value: object) -> str:
    """Normalize district names for accent-insensitive joins."""
    text = "".join(
        character
        for character in unicodedata.normalize("NFD", str(value or ""))
        if unicodedata.category(character) != "Mn"
    )
    return re.sub(r"\s+", " ", text).strip().upper()


def find_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    """Find a column despite capitalization, underscores, or accents."""
    normalized_columns = {normalize(column).replace("_", ""): column for column in columns}
    for candidate in candidates:
        resolved = normalized_columns.get(normalize(candidate).replace("_", ""))
        if resolved:
            return resolved
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a high-resolution binned Lima rental choropleth from Gold CSV data."
    )
    parser.add_argument("input_csv", type=Path, help="CSV exported from Gold market_daily_by_district")
    parser.add_argument(
        "--boundaries",
        type=Path,
        required=True,
        help="Official district Shapefile (.shp) or its downloaded .zip",
    )
    parser.add_argument(
        "--district-column",
        help="District-name field in the Shapefile; inferred by default",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/lima_rental_choropleth.png"),
        help="Output PNG path (default: reports/lima_rental_choropleth.png)",
    )
    parser.add_argument(
        "--label-top",
        type=int,
        default=0,
        help="Number of districts with visible labels; use 0 for none (default: 0)",
    )
    parser.add_argument(
        "--bins",
        type=float,
        nargs="+",
        default=[1500, 2000, 2500, 3000, 4000],
        help="Upper boundaries for rent brackets in PEN (default: 1500 2000 2500 3000 4000)",
    )
    parser.add_argument(
        "--scrape-date",
        help="Scrape/ingest date in YYYY-MM-DD; inferred from ingest_date when present",
    )
    return parser.parse_args()


def import_plotting_libraries():
    """Delay optional imports so a missing dependency produces a useful error."""
    try:
        import geopandas as gpd
        import matplotlib.pyplot as plt
        import numpy as np
        import pandas as pd
        from matplotlib.patches import Patch
        from matplotlib.ticker import FuncFormatter
    except ImportError as error:
        raise RuntimeError("Missing map dependencies. Run: uv sync") from error
    return gpd, plt, np, pd, Patch, FuncFormatter


def load_market_data(input_csv: Path, pd):
    market = pd.read_csv(input_csv)
    missing = REQUIRED_COLUMNS - set(market.columns)
    if missing:
        raise ValueError(
            "Gold export CSV missing columns: " + ", ".join(sorted(missing))
        )

    detected_dates = []
    if "ingest_date" in market.columns:
        detected_dates = sorted(
            market["ingest_date"].dropna().astype(str).str[:10].unique().tolist()
        )
        if len(detected_dates) > 1:
            raise ValueError(
                "CSV contains multiple ingest_date values; export one snapshot or use a filtered query."
            )

    market = market.loc[market["currency"].astype(str).str.upper() == "PEN"].copy()
    market = market.loc[
        :,
        [
            "district",
            "bedrooms",
            "listing_count",
            "avg_price",
            "median_price",
            "avg_area_m2",
            "median_price_per_m2",
            "new_listing_count",
            "removed_listing_count",
            "price_changed_count",
            "offer_change_pct",
        ],
    ].copy()
    market["district"] = market["district"].astype(str).str.strip()
    for column in (
        "listing_count",
        "avg_price",
        "median_price",
        "avg_area_m2",
        "median_price_per_m2",
        "new_listing_count",
        "removed_listing_count",
        "price_changed_count",
        "offer_change_pct",
    ):
        market[column] = pd.to_numeric(market[column], errors="coerce")
    market = market.dropna(
        subset=[
            "district",
            "bedrooms",
            "listing_count",
            "avg_price",
            "median_price",
            "avg_area_m2",
            "median_price_per_m2",
        ]
    )
    market = market.loc[market["listing_count"] > 0].copy()
    market["bedrooms"] = pd.to_numeric(market["bedrooms"], errors="coerce")
    market = market.loc[market["bedrooms"].between(1, 4)].copy()
    market["bedrooms"] = market["bedrooms"].astype(int)

    market["district_key"] = market["district"].map(normalize).replace(DISTRICT_ALIASES)
    for metric in ("avg_price", "median_price", "avg_area_m2", "median_price_per_m2"):
        market[f"{metric}_weight"] = market[metric] * market["listing_count"]
    bedroom_market = market.groupby(["district_key", "bedrooms"], as_index=False).agg(
        district=("district", "first"),
        listing_count=("listing_count", "sum"),
        avg_price_weight=("avg_price_weight", "sum"),
        median_price_weight=("median_price_weight", "sum"),
        avg_area_m2_weight=("avg_area_m2_weight", "sum"),
        median_price_per_m2_weight=("median_price_per_m2_weight", "sum"),
        new_listing_count=("new_listing_count", "sum"),
        removed_listing_count=("removed_listing_count", "sum"),
        price_changed_count=("price_changed_count", "sum"),
    )
    bedroom_market["avg_rent_pen"] = bedroom_market["avg_price_weight"] / bedroom_market["listing_count"]
    bedroom_market["median_price"] = bedroom_market["median_price_weight"] / bedroom_market["listing_count"]
    bedroom_market["avg_area_m2"] = bedroom_market["avg_area_m2_weight"] / bedroom_market["listing_count"]
    bedroom_market["median_price_m2"] = bedroom_market["median_price_per_m2_weight"] / bedroom_market["listing_count"]
    bedroom_market = bedroom_market.drop(
        columns=[
            "avg_price_weight",
            "median_price_weight",
            "avg_area_m2_weight",
            "median_price_per_m2_weight",
        ]
    )

    market = market.groupby("district_key", as_index=False).agg(
        district=("district", "first"),
        listing_count=("listing_count", "sum"),
        avg_price_weight=("avg_price_weight", "sum"),
        median_price_weight=("median_price_weight", "sum"),
        avg_area_m2_weight=("avg_area_m2_weight", "sum"),
        median_price_per_m2_weight=("median_price_per_m2_weight", "sum"),
    )
    market["avg_rent_pen"] = market["avg_price_weight"] / market["listing_count"]
    market["median_price"] = market["median_price_weight"] / market["listing_count"]
    market["avg_area_m2"] = market["avg_area_m2_weight"] / market["listing_count"]
    market["median_price_m2"] = market["median_price_per_m2_weight"] / market["listing_count"]
    return (
        market.drop(
            columns=[
                "avg_price_weight",
                "median_price_weight",
                "avg_area_m2_weight",
                "median_price_per_m2_weight",
            ]
        ),
        detected_dates[0] if detected_dates else None,
        bedroom_market,
    )


def validate_scrape_date(value: str | None) -> str:
    """Validate and normalize the date displayed by both visualizations."""
    if not value:
        raise ValueError(
            "Scrape date is unavailable. Add ingest_date to the CSV or pass --scrape-date YYYY-MM-DD."
        )
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as error:
        raise ValueError("Scrape date must use YYYY-MM-DD.") from error


def load_boundaries(boundaries_path: Path, district_column: str | None, gpd):
    if not boundaries_path.exists():
        raise FileNotFoundError(f"Shapefile not found: {boundaries_path}")

    source = f"zip://{boundaries_path.resolve()}" if boundaries_path.suffix.lower() == ".zip" else boundaries_path
    boundaries = gpd.read_file(source)
    if boundaries.empty:
        raise ValueError("The Shapefile contains no features.")
    if boundaries.crs is None:
        raise ValueError("The Shapefile has no CRS. Use a georeferenced district layer.")

    selected_column = district_column or find_column(
        list(boundaries.columns), DISTRICT_COLUMN_CANDIDATES
    )
    if selected_column not in boundaries.columns:
        available = ", ".join(map(str, boundaries.columns))
        raise ValueError(
            "Could not identify the district-name column. Use --district-column. "
            f"Available columns: {available}"
        )

    department_column = find_column(list(boundaries.columns), DEPARTMENT_COLUMN_CANDIDATES)
    if department_column:
        department_key = boundaries[department_column].map(normalize)
        scoped = boundaries.loc[department_key.isin(LIMA_METROPOLITANA_DEPARTMENTS)].copy()
        if not scoped.empty:
            boundaries = scoped

    province_column = find_column(list(boundaries.columns), PROVINCE_COLUMN_CANDIDATES)
    if province_column:
        province_key = boundaries[province_column].map(normalize)
        scoped = boundaries.loc[province_key.isin(LIMA_METROPOLITANA_PROVINCES)].copy()
        if not scoped.empty:
            boundaries = scoped

    boundaries = boundaries.loc[boundaries.geometry.notna()].copy()
    boundaries["district_key"] = boundaries[selected_column].map(normalize)
    boundaries = boundaries.loc[boundaries["district_key"] != ""].copy()
    return boundaries.dissolve(by="district_key", as_index=False)[["district_key", "geometry"]]


def rent_bracket_labels(bins: list[float]) -> list[str]:
    """Build human-readable labels for fixed rent bands."""
    labels = [f"Menos de S/ {bins[0]:,.0f}"]
    labels.extend(
        f"S/ {lower:,.0f} – {upper - 1:,.0f}"
        for lower, upper in zip(bins, bins[1:])
    )
    labels.append(f"S/ {bins[-1]:,.0f} o más")
    return labels


def plot_choropleth(
    boundaries,
    market,
    bedroom_market,
    output: Path,
    label_top: int,
    bins: list[float],
    scrape_date: str,
    plt,
    np,
    Patch,
    FuncFormatter,
):
    joined = boundaries.merge(market, on="district_key", how="left")
    matched = joined.loc[joined["listing_count"].notna()].copy()
    if matched.empty:
        raise ValueError("No Gold district matched a district in the Shapefile.")

    # Web Mercator avoids the visible horizontal distortion caused by plotting
    # geographic degrees directly.
    base_plot = joined.to_crs(epsg=3857)
    matched_plot = matched.to_crs(epsg=3857)
    labels = rent_bracket_labels(bins)
    matched_plot["rent_bracket"] = matched_plot["avg_rent_pen"].map(
        lambda value: next(
            (index for index, boundary in enumerate(bins) if value < boundary),
            len(bins),
        )
    )
    if len(labels) != len(RENT_COLORS):
        colour_map = plt.get_cmap("Blues", len(labels) + 1)
        colours = [colour_map(index + 1) for index in range(len(labels))]
    else:
        colours = list(RENT_COLORS)

    figure = plt.figure(figsize=(17, 10))
    grid = figure.add_gridspec(
        2,
        3,
        width_ratios=(1.18, 1.18, 1.0),
        height_ratios=(0.92, 1.08),
    )
    map_axis = figure.add_subplot(grid[:, :2])
    opportunity_axis = figure.add_subplot(grid[0, 2])
    ranking_axis = figure.add_subplot(grid[1, 2])
    figure.patch.set_facecolor("white")
    map_axis.set_facecolor("#f7f9fb")
    base_plot.plot(
        ax=map_axis,
        color="#e5e7eb",
        edgecolor="#94a3b8",
        linewidth=0.6,
        zorder=1,
    )
    for bracket, label in enumerate(labels):
        subset = matched_plot.loc[matched_plot["rent_bracket"] == bracket]
        if not subset.empty:
            subset.plot(
                ax=map_axis,
                color=colours[bracket],
                edgecolor="#ffffff",
                linewidth=0.8,
                zorder=2,
            )

    top_districts = matched_plot.nlargest(max(0, label_top), "listing_count").copy()
    top_points = top_districts.geometry.representative_point()
    for (_, district), point in zip(top_districts.iterrows(), top_points):
        map_axis.annotate(
            district["district"],
            xy=(point.x, point.y),
            xytext=(7, 7),
            textcoords="offset points",
            fontsize=9,
            fontweight="bold",
            color="#111827",
            bbox={"boxstyle": "round,pad=0.22", "fc": "white", "ec": "none", "alpha": 0.82},
            zorder=4,
        )

    legend_handles = [
        Patch(facecolor=colours[index], edgecolor="#ffffff", label=label)
        for index, label in enumerate(labels)
    ]
    legend_handles.append(Patch(facecolor="#e5e7eb", edgecolor="#94a3b8", label="Sin dato PEN"))
    map_axis.legend(
        handles=legend_handles,
        title="Alquiler promedio mensual",
        loc="lower left",
        frameon=True,
        facecolor="white",
        edgecolor="#cbd5e1",
        fontsize=8.5,
        title_fontsize=9.5,
    )

    mapped_listings = int(matched_plot["listing_count"].sum())
    map_axis.set_title("Mapa coroplético · alquiler promedio mensual (PEN)", loc="left", fontsize=12, fontweight="bold", pad=10)
    map_axis.set_axis_off()

    # Commercial opportunity matrix: a district is most interesting when it
    # combines market depth (supply) and a high rental ticket.
    supply_median = float(np.median(matched_plot["listing_count"]))
    rent_median = float(np.median(matched_plot["avg_rent_pen"]))
    x_max = float(matched_plot["listing_count"].max()) * 1.13
    y_min = float(matched_plot["avg_rent_pen"].min()) * 0.88
    y_max = float(matched_plot["avg_rent_pen"].max()) * 1.10
    opportunity_axis.fill_between(
        [supply_median, x_max],
        [rent_median, rent_median],
        [y_max, y_max],
        color="#dbeafe",
        alpha=0.55,
        zorder=0,
    )
    opportunity_axis.scatter(
        matched_plot["listing_count"],
        matched_plot["avg_rent_pen"],
        s=62,
        c=[colours[int(bracket)] for bracket in matched_plot["rent_bracket"]],
        edgecolors="#334155",
        linewidths=0.65,
        alpha=0.92,
        zorder=2,
    )
    opportunity_axis.axvline(supply_median, color="#64748b", linewidth=1.0, linestyle="--")
    opportunity_axis.axhline(rent_median, color="#64748b", linewidth=1.0, linestyle="--")
    opportunity_axis.set_xlim(0, x_max)
    opportunity_axis.set_ylim(y_min, y_max)
    opportunity_axis.text(
        x_max * 0.98,
        y_max * 0.98,
        "Prioridad comercial\noferta + ticket altos",
        ha="right",
        va="top",
        fontsize=8.5,
        color="#1e3a8a",
    )
    opportunity = matched_plot.loc[
        (matched_plot["listing_count"] >= supply_median)
        & (matched_plot["avg_rent_pen"] >= rent_median)
    ].copy()
    opportunity["commercial_value"] = opportunity["listing_count"] * opportunity["avg_rent_pen"]
    opportunity = opportunity.nlargest(5, "commercial_value")
    offsets = ((6, 6), (6, -12), (-8, 7), (6, 7), (-8, -12))
    for ((_, district), offset) in zip(opportunity.iterrows(), offsets):
        opportunity_axis.annotate(
            district["district"],
            xy=(district["listing_count"], district["avg_rent_pen"]),
            xytext=offset,
            textcoords="offset points",
            fontsize=8,
            color="#1f2937",
            ha="right" if offset[0] < 0 else "left",
        )
    opportunity_axis.set_title("Matriz de oportunidad comercial", loc="left", fontsize=11.5, fontweight="bold", pad=10)
    opportunity_axis.set_xlabel("Oferta disponible (avisos)", fontsize=9)
    opportunity_axis.set_ylabel("Alquiler promedio", fontsize=9)
    opportunity_axis.yaxis.set_major_formatter(FuncFormatter(lambda value, _position: f"S/ {value / 1000:.1f}k"))
    opportunity_axis.grid(color="#e2e8f0", linewidth=0.7)
    opportunity_axis.set_axisbelow(True)
    opportunity_axis.spines[["top", "right"]].set_visible(False)
    opportunity_axis.spines[["bottom", "left"]].set_color("#cbd5e1")

    # The supply ranking makes territory allocation immediately actionable.
    ranking = matched_plot.nlargest(10, "listing_count").sort_values("listing_count", ascending=True)
    if bedroom_market is not None:
        bedroom_mix = (
            bedroom_market.loc[bedroom_market["district_key"].isin(ranking["district_key"])]
            .pivot_table(
                index="district_key",
                columns="bedrooms",
                values="listing_count",
                aggfunc="sum",
                fill_value=0,
            )
            .reindex(ranking["district_key"])
            .fillna(0)
        )
        left = np.zeros(len(ranking))
        for bedrooms in range(1, 5):
            values = (
                bedroom_mix[bedrooms].to_numpy(dtype=float)
                if bedrooms in bedroom_mix.columns
                else np.zeros(len(ranking))
            )
            ranking_axis.barh(
                ranking["district"],
                values,
                left=left,
                color=BEDROOM_COLORS[bedrooms],
                edgecolor="#ffffff",
                linewidth=0.7,
                label=f"{bedrooms} cuarto{'s' if bedrooms > 1 else ''}",
            )
            left += values
        for y_position, (_, district) in enumerate(ranking.iterrows()):
            ranking_axis.text(
                district["listing_count"] + 2,
                y_position,
                f"{district['listing_count']:,.0f} · S/ {district['avg_rent_pen']:,.0f} prom.",
                va="center",
                fontsize=8.2,
                color="#334155",
            )
        ranking_axis.legend(
            title="Número de cuartos",
            loc="lower right",
            fontsize=7.8,
            title_fontsize=8.5,
            frameon=False,
        )
        ranking_title = "Oferta por distrito y número de cuartos\nTotal de avisos · etiqueta = ticket promedio"
    else:
        ranking_colours = [colours[int(bracket)] for bracket in ranking["rent_bracket"]]
        bars = ranking_axis.barh(
            ranking["district"],
            ranking["listing_count"],
            color=ranking_colours,
            edgecolor="#ffffff",
            linewidth=0.8,
        )
        ranking_axis.bar_label(
            bars,
            labels=[
                f"{count:,.0f} · S/ {rent:,.0f} prom."
                for rent, count in zip(ranking["avg_rent_pen"], ranking["listing_count"])
            ],
            padding=4,
            fontsize=8.5,
            color="#334155",
        )
        ranking_title = "Dónde se concentra la oferta\nCSV sin dimensión bedrooms"
    ranking_axis.set_xlim(0, ranking["listing_count"].max() * 1.52)
    ranking_axis.set_title(
        ranking_title,
        loc="left",
        fontsize=11.5,
        fontweight="bold",
        pad=10,
    )
    ranking_axis.set_xlabel("Cantidad de avisos", fontsize=9)
    ranking_axis.xaxis.set_major_formatter(FuncFormatter(lambda value, _position: f"{value:,.0f}"))
    ranking_axis.grid(axis="x", color="#e2e8f0", linewidth=0.7)
    ranking_axis.set_axisbelow(True)
    ranking_axis.spines[["top", "right", "left"]].set_visible(False)
    ranking_axis.spines["bottom"].set_color("#cbd5e1")
    ranking_axis.tick_params(axis="y", labelsize=9)

    figure.suptitle(
        "Mercado de alquiler residencial de Lima Metropolitana",
        fontsize=20,
        fontweight="bold",
        x=0.04,
        y=0.975,
        ha="left",
    )
    figure.text(
        0.04,
        0.925,
        f"Scrape: {scrape_date} · Dónde está la oferta, cuánto vale y dónde priorizar ventas · {mapped_listings:,.0f} avisos PEN en {len(matched_plot)} distritos",
        fontsize=10.5,
        color="#475569",
    )
    figure.text(
        0.04,
        0.025,
        "Fuente: Gold market_daily_by_district · Límites: Shapefile IGN/IDEP · Sin moneda UNKNOWN · Promedios ponderados por avisos",
        fontsize=8.5,
        color="#64748b",
    )
    figure.subplots_adjust(
        left=0.04,
        right=0.97,
        bottom=0.09,
        top=0.84,
        wspace=0.42,
        hspace=0.56,
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output, dpi=220, bbox_inches="tight", facecolor=figure.get_facecolor())
    plt.close(figure)

    unmatched = market.loc[~market["district_key"].isin(joined["district_key"]), "district"].tolist()
    return len(matched_plot), mapped_listings, unmatched


def main() -> int:
    args = parse_args()
    try:
        gpd, plt, np, pd, Patch, FuncFormatter = import_plotting_libraries()
        if args.bins != sorted(set(args.bins)):
            raise ValueError("--bins must contain unique values in ascending order.")
        market, detected_scrape_date, bedroom_market = load_market_data(args.input_csv, pd)
        scrape_date = validate_scrape_date(args.scrape_date or detected_scrape_date)
        boundaries = load_boundaries(args.boundaries, args.district_column, gpd)
        mapped_districts, mapped_listings, unmatched = plot_choropleth(
            boundaries,
            market,
            bedroom_market,
            args.output,
            args.label_top,
            args.bins,
            scrape_date,
            plt,
            np,
            Patch,
            FuncFormatter,
        )
    except (OSError, RuntimeError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    print(f"Created: {args.output.resolve()}")
    print(f"Mapped: {mapped_listings:,.0f} listings in {mapped_districts} districts")
    if unmatched:
        print("Unmatched location labels (first 10): " + ", ".join(unmatched[:10]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
