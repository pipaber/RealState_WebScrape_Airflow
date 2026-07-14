# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: validación contractual
# MAGIC
# MAGIC Verifica los granos publicados y evita declarar anuncios retirados cuando
# MAGIC falta alguno de los cuatro segmentos exitosos del snapshot actual.

# COMMAND ----------

# MAGIC %run ./config/parameters

# COMMAND ----------

from datetime import datetime

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text(
    "processing_date",
    "",
    "Fecha de snapshot YYYY-MM-DD (vacío = última fecha publicada en Gold)",
)


def _resolve_processing_date() -> str:
    requested_date = dbutils.widgets.get("processing_date").strip()
    if requested_date:
        return datetime.strptime(requested_date, "%Y-%m-%d").date().isoformat()

    latest = (
        spark.table(params["gold_market_daily_table"])
        .agg(F.max("ingest_date").alias("processing_date"))
        .first()["processing_date"]
    )
    if latest is None:
        raise ValueError("No hay indicadores Gold para validar.")
    return latest.isoformat()


processing_date = _resolve_processing_date()
processing_date_col = F.lit(processing_date).cast("date")

market = spark.table(params["gold_market_daily_table"]).where(
    F.col("ingest_date") == processing_date_col
)
changes = spark.table(params["gold_listing_changes_table"]).where(
    F.col("ingest_date") == processing_date_col
)
latest = spark.table(params["gold_listing_latest_table"])

duplicate_market_keys = (
    market.groupBy("ingest_date", "district", "bedrooms", "currency")
    .count()
    .where(F.col("count") > 1)
    .count()
)
duplicate_change_keys = (
    changes.groupBy("ingest_date", "listing_id", "change_type")
    .count()
    .where(F.col("count") > 1)
    .count()
)
duplicate_latest_keys = latest.groupBy("listing_id").count().where(F.col("count") > 1).count()
invalid_latest_lifecycle = latest.where(
    F.col("first_seen_date").isNull()
    | F.col("last_seen_date").isNull()
    | (F.col("first_seen_date") > F.col("last_seen_date"))
).count()

successful_segments = (
    spark.table(params["bronze_runs_table"])
    .where(
        (F.lower(F.col("status")) == F.lit("success"))
        & (F.col("_partition_ingest_date") == processing_date_col)
    )
    .select(F.col("search_params.bedrooms").alias("bedrooms"))
    .where(F.col("bedrooms").between(1, 4))
    .dropDuplicates()
    .count()
)
removed_listings = changes.where(F.col("change_type") == "REMOVED").count()

validation_metrics = spark.createDataFrame(
    [
        (processing_date, "market_rows", market.count()),
        (processing_date, "change_rows", changes.count()),
        (processing_date, "current_active_listings", latest.where(F.col("is_active")).count()),
        (processing_date, "duplicate_market_keys", duplicate_market_keys),
        (processing_date, "duplicate_change_keys", duplicate_change_keys),
        (processing_date, "duplicate_latest_keys", duplicate_latest_keys),
        (processing_date, "invalid_latest_lifecycle", invalid_latest_lifecycle),
        (processing_date, "successful_bedroom_segments", successful_segments),
        (processing_date, "removed_listings", removed_listings),
    ],
    ["processing_date", "metric", "value"],
)
display(validation_metrics)

violations = {
    "duplicate_market_keys": duplicate_market_keys,
    "duplicate_change_keys": duplicate_change_keys,
    "duplicate_latest_keys": duplicate_latest_keys,
    "invalid_latest_lifecycle": invalid_latest_lifecycle,
    "removed_without_complete_snapshot": int(successful_segments < 4 and removed_listings > 0),
}
failed_checks = {name: value for name, value in violations.items() if value > 0}

if failed_checks:
    raise AssertionError(f"Contrato Gold incumplido para {processing_date}: {failed_checks}")

print(f"Contrato Gold validado correctamente para {processing_date}.")
