# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: indicadores, estado actual y cambios diarios
# MAGIC
# MAGIC Publica los tres outputs del Data Product a partir de Silver. Las salidas
# MAGIC son tablas Delta para poder sincronizar una fecha reejecutada sin crear
# MAGIC duplicados. Los retiros solo se calculan cuando los cuatro segmentos de
# MAGIC dormitorios tienen manifests exitosos para el snapshot.

# COMMAND ----------

# MAGIC %run ./config/parameters

# COMMAND ----------

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window, functions as F

# COMMAND ----------

dbutils.widgets.text(
    "processing_date",
    "",
    "Fecha de snapshot YYYY-MM-DD (vacío = última fecha publicada en Silver)",
)


def _resolve_processing_date() -> str:
    requested_date = dbutils.widgets.get("processing_date").strip()
    if requested_date:
        return datetime.strptime(requested_date, "%Y-%m-%d").date().isoformat()

    latest = (
        spark.table(params["silver_snapshot_table"])
        .agg(F.max("ingest_date").alias("processing_date"))
        .first()["processing_date"]
    )
    if latest is None:
        raise ValueError("No hay snapshots Silver disponibles para publicar Gold.")
    return latest.isoformat()


processing_date = _resolve_processing_date()
processing_date_col = F.lit(processing_date).cast("date")
print(f"Publicando Gold para ingest_date={processing_date}")

# COMMAND ----------

def _table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def _ensure_delta_table(df: DataFrame, table_name: str) -> None:
    if not _table_exists(table_name):
        df.limit(0).write.format("delta").saveAsTable(table_name)


def _sync_date(
    source_df: DataFrame,
    target_table: str,
    merge_condition: str,
) -> None:
    """Sincroniza por MERGE una fecha, borrando resultados obsoletos de esa fecha."""
    _ensure_delta_table(source_df, target_table)
    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(
            condition=f"target.ingest_date = DATE('{processing_date}')"
        )
        .execute()
    )


def _sync_full(source_df: DataFrame, target_table: str, merge_condition: str) -> None:
    """Sincroniza una tabla derivada completa, como el estado actual de anuncios."""
    _ensure_delta_table(source_df, target_table)
    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )


def _market_dimensions(df: DataFrame) -> DataFrame:
    """Establece las dimensiones con las que se publica el mercado diario."""
    return (
        df.withColumn("district", F.coalesce(F.col("district"), F.lit("Unknown")))
        .withColumn("currency", F.coalesce(F.col("currency"), F.lit("UNKNOWN")))
    )


# COMMAND ----------

all_snapshots = spark.table(params["silver_snapshot_table"])
current_snapshot = all_snapshots.where(F.col("ingest_date") == processing_date_col)

if current_snapshot.limit(1).count() == 0:
    raise ValueError(f"No hay filas Silver para {processing_date}.")

previous_date = (
    all_snapshots.where(F.col("ingest_date") < processing_date_col)
    .agg(F.max("ingest_date").alias("previous_date"))
    .first()["previous_date"]
)
previous_date_text = previous_date.isoformat() if previous_date else None

previous_snapshot = (
    all_snapshots.where(F.col("ingest_date") == F.lit(previous_date_text).cast("date"))
    if previous_date_text
    else current_snapshot.limit(0)
)
historical_listing_ids = (
    all_snapshots.where(F.col("ingest_date") < processing_date_col)
    .select("listing_id")
    .dropDuplicates()
)

successful_segments = (
    spark.table(params["bronze_runs_table"])
    .where(
        (F.lower(F.col("status")) == F.lit("success"))
        & (F.col("_partition_ingest_date") == processing_date_col)
    )
    .select(F.col("search_params.bedrooms").alias("bedrooms"))
    .where(F.col("bedrooms").between(1, 4))
    .dropDuplicates()
)
complete_snapshot = successful_segments.count() == 4
print(
    f"Snapshot completo de cuatro segmentos: {complete_snapshot}. "
    "Los retiros solo se publican cuando es True."
)

# COMMAND ----------

current_for_change = current_snapshot.select(
    F.col("listing_id").alias("current_listing_id"),
    F.col("district").alias("current_district"),
    F.col("city").alias("current_city"),
    F.col("bedrooms_min").alias("current_bedrooms_min"),
    F.col("bedrooms_max").alias("current_bedrooms_max"),
    F.col("currency").alias("current_currency"),
    F.col("price_amount").alias("current_price"),
    F.col("ingest_date").alias("current_seen_date"),
)
previous_for_change = previous_snapshot.select(
    F.col("listing_id").alias("previous_listing_id"),
    F.col("district").alias("previous_district"),
    F.col("city").alias("previous_city"),
    F.col("bedrooms_min").alias("previous_bedrooms_min"),
    F.col("bedrooms_max").alias("previous_bedrooms_max"),
    F.col("currency").alias("previous_currency"),
    F.col("price_amount").alias("previous_price"),
    F.col("ingest_date").alias("previous_seen_date"),
)

comparison = (
    current_for_change.join(
        previous_for_change,
        F.col("current_listing_id") == F.col("previous_listing_id"),
        "full_outer",
    )
    .withColumn("listing_id", F.coalesce(F.col("current_listing_id"), F.col("previous_listing_id")))
    .join(historical_listing_ids.withColumn("was_seen_before", F.lit(True)), "listing_id", "left")
    .withColumn("district", F.coalesce(F.col("current_district"), F.col("previous_district"), F.lit("Unknown")))
    .withColumn("city", F.coalesce(F.col("current_city"), F.col("previous_city"), F.lit("Lima")))
    .withColumn("bedrooms_min", F.coalesce(F.col("current_bedrooms_min"), F.col("previous_bedrooms_min")))
    .withColumn("bedrooms_max", F.coalesce(F.col("current_bedrooms_max"), F.col("previous_bedrooms_max")))
    .withColumn("currency", F.coalesce(F.col("current_currency"), F.col("previous_currency"), F.lit("UNKNOWN")))
    .withColumn(
        "change_type",
        F.when(
            F.col("current_listing_id").isNotNull()
            & F.col("previous_listing_id").isNull()
            & F.col("was_seen_before").isNotNull(),
            F.lit("REAPPEARED"),
        )
        .when(
            F.col("current_listing_id").isNotNull() & F.col("previous_listing_id").isNull(),
            F.lit("NEW"),
        )
        .when(
            F.col("current_listing_id").isNotNull()
            & F.col("previous_listing_id").isNotNull()
            & F.col("current_price").isNotNull()
            & F.col("previous_price").isNotNull()
            & (F.col("current_price") > F.col("previous_price")),
            F.lit("PRICE_INCREASE"),
        )
        .when(
            F.col("current_listing_id").isNotNull()
            & F.col("previous_listing_id").isNotNull()
            & F.col("current_price").isNotNull()
            & F.col("previous_price").isNotNull()
            & (F.col("current_price") < F.col("previous_price")),
            F.lit("PRICE_DECREASE"),
        )
        .when(
            F.col("current_listing_id").isNotNull() & F.col("previous_listing_id").isNotNull(),
            F.lit("UNCHANGED"),
        )
        .when(
            F.col("current_listing_id").isNull()
            & F.col("previous_listing_id").isNotNull()
            & F.lit(complete_snapshot),
            F.lit("REMOVED"),
        ),
    )
    .where(F.col("change_type").isNotNull())
)

change_rows = comparison.select(
    processing_date_col.alias("ingest_date"),
    "listing_id",
    "district",
    "city",
    "bedrooms_min",
    "bedrooms_max",
    "currency",
    "change_type",
    F.col("previous_price").cast("double").alias("previous_price"),
    F.col("current_price").cast("double").alias("current_price"),
    F.when(
        F.col("previous_price").isNotNull() & F.col("current_price").isNotNull(),
        F.col("current_price") - F.col("previous_price"),
    ).alias("price_change_amount"),
    F.when(
        F.col("previous_price") > 0,
        F.round(
            (F.col("current_price") - F.col("previous_price")) / F.col("previous_price") * F.lit(100.0),
            2,
        ),
    ).alias("price_change_pct"),
    "previous_seen_date",
    "current_seen_date",
    F.current_timestamp().alias("calculated_at"),
)

_sync_date(
    change_rows,
    params["gold_listing_changes_table"],
    "target.ingest_date = source.ingest_date "
    "AND target.listing_id = source.listing_id "
    "AND target.change_type = source.change_type",
)

# COMMAND ----------

current_options = _market_dimensions(
    spark.table(params["silver_bedroom_options_view"]).where(F.col("ingest_date") == processing_date_col)
)
previous_options = _market_dimensions(
    spark.table(params["silver_bedroom_options_view"]).where(
        F.col("ingest_date") == F.lit(previous_date_text).cast("date")
    )
    if previous_date_text
    else current_options.limit(0)
)

market_current = current_options.groupBy("ingest_date", "district", "bedrooms", "currency").agg(
    F.countDistinct("listing_id").alias("listing_count"),
    F.avg("price_amount").alias("avg_price"),
    F.percentile_approx("price_amount", 0.5).alias("median_price"),
    F.min("price_amount").alias("min_price"),
    F.max("price_amount").alias("max_price"),
    F.avg("area_avg_m2").alias("avg_area_m2"),
    F.percentile_approx("price_per_m2", 0.5).alias("median_price_per_m2"),
)
market_previous = previous_options.groupBy("district", "bedrooms", "currency").agg(
    F.countDistinct("listing_id").alias("previous_listing_count")
)

change_options = (
    change_rows.where(
        F.col("bedrooms_min").between(1, 4)
        & F.col("bedrooms_max").between(F.col("bedrooms_min"), 4)
    )
    .withColumn("bedrooms", F.explode(F.sequence(F.col("bedrooms_min"), F.col("bedrooms_max"))))
)
change_metrics = change_options.groupBy("ingest_date", "district", "bedrooms", "currency").agg(
    F.countDistinct(F.when(F.col("change_type") == "NEW", F.col("listing_id"))).alias("new_listing_count"),
    F.countDistinct(F.when(F.col("change_type") == "REMOVED", F.col("listing_id"))).alias("removed_listing_count"),
    F.countDistinct(
        F.when(F.col("change_type").isin("PRICE_INCREASE", "PRICE_DECREASE"), F.col("listing_id"))
    ).alias("price_changed_count"),
)

market_with_changes = (
    market_current.alias("market")
    .join(
        change_metrics.alias("changes"),
        ["ingest_date", "district", "bedrooms", "currency"],
        "full_outer",
    )
    .select(
        F.coalesce(F.col("market.ingest_date"), F.col("changes.ingest_date")).alias("ingest_date"),
        F.coalesce(F.col("market.district"), F.col("changes.district")).alias("district"),
        F.coalesce(F.col("market.bedrooms"), F.col("changes.bedrooms")).alias("bedrooms"),
        F.coalesce(F.col("market.currency"), F.col("changes.currency")).alias("currency"),
        F.coalesce(F.col("market.listing_count"), F.lit(0)).cast("long").alias("listing_count"),
        "avg_price",
        "median_price",
        "min_price",
        "max_price",
        "avg_area_m2",
        "median_price_per_m2",
        F.coalesce(F.col("changes.new_listing_count"), F.lit(0)).cast("long").alias("new_listing_count"),
        F.coalesce(F.col("changes.removed_listing_count"), F.lit(0)).cast("long").alias("removed_listing_count"),
        F.coalesce(F.col("changes.price_changed_count"), F.lit(0)).cast("long").alias("price_changed_count"),
    )
)

market_daily = (
    market_with_changes.alias("current")
    .join(market_previous.alias("previous"), ["district", "bedrooms", "currency"], "left")
    .withColumn(
        "offer_change_pct",
        F.when(
            F.col("previous.previous_listing_count") > 0,
            F.round(
                (F.col("listing_count") - F.col("previous.previous_listing_count"))
                / F.col("previous.previous_listing_count")
                * F.lit(100.0),
                2,
            ),
        ),
    )
    .withColumn("calculated_at", F.current_timestamp())
    .select(
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
    )
)

_sync_date(
    market_daily,
    params["gold_market_daily_table"],
    "target.ingest_date = source.ingest_date "
    "AND target.district = source.district "
    "AND target.bedrooms = source.bedrooms "
    "AND target.currency = source.currency",
)

# COMMAND ----------

latest_window = Window.partitionBy("listing_id").orderBy(
    F.col("ingest_date").desc(),
    F.col("observed_at").desc_nulls_last(),
    F.col("run_id").desc(),
)
lifecycle = all_snapshots.groupBy("listing_id").agg(
    F.min("ingest_date").alias("first_seen_date"),
    F.max("ingest_date").alias("last_seen_date"),
    F.countDistinct("ingest_date").alias("days_observed"),
)
latest_snapshot_date = all_snapshots.agg(F.max("ingest_date").alias("latest_snapshot_date")).first()[
    "latest_snapshot_date"
]

latest_rows = (
    all_snapshots.withColumn("_latest_rank", F.row_number().over(latest_window))
    .where(F.col("_latest_rank") == 1)
    .join(lifecycle, "listing_id", "inner")
    .select(
        "listing_id",
        "listing_type",
        "url",
        "district",
        "city",
        F.col("bedrooms_min").alias("bedrooms"),
        "bathrooms",
        F.col("area_avg_m2").alias("area_m2"),
        "currency",
        F.col("price_amount").alias("price_amount"),
        "price_per_m2",
        "features",
        "first_seen_date",
        "last_seen_date",
        "days_observed",
        (F.col("last_seen_date") == F.lit(latest_snapshot_date).cast("date")).alias("is_active"),
        F.col("run_id").alias("last_run_id"),
        F.current_timestamp().alias("updated_at"),
    )
)

_sync_full(
    latest_rows,
    params["gold_listing_latest_table"],
    "target.listing_id = source.listing_id",
)

print(f"Gold publicado correctamente para {processing_date}.")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT bedrooms, currency, SUM(listing_count) AS listings, AVG(avg_price) AS avg_segment_price
        FROM {params['gold_market_daily_table']}
        WHERE ingest_date = DATE('{processing_date}')
        GROUP BY bedrooms, currency
        ORDER BY bedrooms, currency
        """
    )
)

display(
    spark.sql(
        f"""
        SELECT change_type, COUNT(*) AS listings
        FROM {params['gold_listing_changes_table']}
        WHERE ingest_date = DATE('{processing_date}')
        GROUP BY change_type
        ORDER BY change_type
        """
    )
)
