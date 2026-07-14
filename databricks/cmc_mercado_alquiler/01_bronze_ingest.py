# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingesta incremental de Urbania
# MAGIC
# MAGIC Ingiere los NDJSON y manifiestos publicados por Airflow. Usa Auto Loader
# MAGIC con checkpoints independientes y `MERGE` sobre claves técnicas, por lo
# MAGIC que reejecutar el notebook no duplica observaciones ni manifests.

# COMMAND ----------

# MAGIC %run ./config/parameters

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

LISTING_SCHEMA = StructType(
    [
        StructField("listing_id", StringType(), True),
        StructField("listing_type", StringType(), True),
        StructField("url", StringType(), True),
        StructField("price_raw", StringType(), True),
        StructField("maintenance_raw", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("price_min", DoubleType(), True),
        StructField("area_raw", StringType(), True),
        StructField("bedrooms_raw", StringType(), True),
        StructField("bathrooms_raw", StringType(), True),
        StructField("units_raw", StringType(), True),
        StructField("address_raw", StringType(), True),
        StructField("street", StringType(), True),
        StructField("district", StringType(), True),
        StructField("city", StringType(), True),
        StructField("features", ArrayType(StringType()), True),
        StructField("description", StringType(), True),
        StructField("image_url", StringType(), True),
        StructField("publisher", StringType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("_source_url", StringType(), True),
        StructField("_page_num", IntegerType(), True),
        StructField("_run_id", StringType(), True),
        StructField(
            "_search_params",
            StructType(
                [
                    StructField("operation", StringType(), True),
                    StructField("property_type", StringType(), True),
                    StructField("location", StringType(), True),
                    StructField("bedrooms", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField("_extraction_method", StringType(), True),
        StructField(
            "_raw",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("posting_type", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("price", StringType(), True),
                    StructField("maintenance", StringType(), True),
                    StructField("features_text", StringType(), True),
                    StructField("street", StringType(), True),
                    StructField("location", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("publisher", StringType(), True),
                    StructField("amenities", ArrayType(StringType()), True),
                    StructField("image", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

MANIFEST_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("records_written", LongType(), True),
        StructField("record_count", LongType(), True),
        StructField("pages_scraped", IntegerType(), True),
        StructField("started_at", StringType(), True),
        StructField("completed_at", StringType(), True),
        StructField("finished_at", StringType(), True),
        StructField(
            "search_params",
            StructType(
                [
                    StructField("operation", StringType(), True),
                    StructField("property_type", StringType(), True),
                    StructField("location", StringType(), True),
                    StructField("bedrooms", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField("data_file", StringType(), True),
        StructField("data_path", StringType(), True),
        StructField("partition", StringType(), True),
        StructField("error", StringType(), True),
    ]
)

# COMMAND ----------

def _file_partition_columns(df: DataFrame) -> DataFrame:
    """Deriva las particiones Hive desde la ruta física del archivo raw."""
    path = F.col("_bronze_source_file")
    bedrooms_from_path = F.regexp_extract(path, r"bedrooms=(\d+)", 1)
    return (
        df.withColumn(
            "_partition_ingest_date",
            F.to_date(F.regexp_extract(path, r"ingest_date=([^/]+)", 1)),
        )
        .withColumn(
            "_partition_bedrooms",
            # regexp_extract returns an empty string when it does not match.
            # Convert only numeric values so an unexpected file path is kept
            # in Bronze with a NULL partition instead of failing the batch.
            F.when(
                bedrooms_from_path.rlike(r"^\d+$"),
                bedrooms_from_path.cast("int"),
            ).otherwise(F.lit(None).cast("int")),
        )
    )


def _merge_insert_only(batch_df: DataFrame, batch_id: int, table_name: str, key_column: str) -> None:
    """Crea la tabla si falta e inserta solo claves que aún no existen."""
    # Do not use batch_df.rdd here: RDD access is unavailable on Spark Connect
    # and some serverless Databricks compute. DataFrame.isEmpty() is supported
    # by the DataFrame API and preserves the same no-op behavior.
    if batch_df.isEmpty():
        print(f"Microbatch {batch_id}: no hay filas para {table_name}.")
        return

    deduplicated = batch_df.dropDuplicates([key_column])

    if not spark.catalog.tableExists(table_name):
        deduplicated.limit(0).write.format("delta").saveAsTable(table_name)

    (
        DeltaTable.forName(spark, table_name)
        .alias("target")
        .merge(deduplicated.alias("source"), f"target.`{key_column}` = source.`{key_column}`")
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Microbatch {batch_id}: procesadas {deduplicated.count()} filas para {table_name}.")


# COMMAND ----------

listings_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", params["listings_schema_location"])
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "_rescued_data")
    .option("pathGlobFilter", "listings_*.jsonl")
    .schema(LISTING_SCHEMA)
    .load(params["raw_source_path"])
    .withColumn("_bronze_source_file", F.col("_metadata.file_path"))
    .withColumn("_bronze_file_modification_time", F.col("_metadata.file_modification_time"))
    .withColumn("_bronze_loaded_at", F.current_timestamp())
)

listings_stream = _file_partition_columns(listings_stream).withColumn(
    "_bronze_observation_key",
    F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("_run_id"), F.lit("missing_run_id")),
            F.coalesce(F.col("listing_id"), F.col("url"), F.lit("missing_listing_id")),
        ),
        256,
    ),
)

(
    listings_stream.writeStream.foreachBatch(
        lambda batch_df, batch_id: _merge_insert_only(
            batch_df,
            batch_id,
            params["bronze_listings_table"],
            "_bronze_observation_key",
        )
    )
    .option("checkpointLocation", params["listings_checkpoint_location"])
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------

manifests_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", params["manifests_schema_location"])
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "_rescued_data")
    .option("pathGlobFilter", "manifest_*.json")
    .schema(MANIFEST_SCHEMA)
    .load(params["raw_source_path"])
    .withColumn("_bronze_source_file", F.col("_metadata.file_path"))
    .withColumn("_bronze_file_modification_time", F.col("_metadata.file_modification_time"))
    .withColumn("_bronze_loaded_at", F.current_timestamp())
)

manifests_stream = _file_partition_columns(manifests_stream).withColumn(
    "records_written",
    F.coalesce(F.col("records_written"), F.col("record_count")),
).withColumn(
    "completed_at",
    F.coalesce(F.col("completed_at"), F.col("finished_at")),
).withColumn(
    "_bronze_manifest_key",
    F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("run_id"), F.lit("missing_run_id")),
            F.coalesce(F.col("_bronze_source_file"), F.lit("missing_source_file")),
        ),
        256,
    ),
)

(
    manifests_stream.writeStream.foreachBatch(
        lambda batch_df, batch_id: _merge_insert_only(
            batch_df,
            batch_id,
            params["bronze_runs_table"],
            "_bronze_manifest_key",
        )
    )
    .option("checkpointLocation", params["manifests_checkpoint_location"])
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
          _partition_bedrooms AS bedrooms,
          COUNT(*) AS raw_observations,
          COUNT(DISTINCT _run_id) AS runs,
          MAX(_bronze_loaded_at) AS latest_bronze_load
        FROM {params['bronze_listings_table']}
        GROUP BY _partition_bedrooms
        ORDER BY bedrooms
        """
    )
)

display(
    spark.sql(
        f"""
        SELECT
          _partition_bedrooms AS bedrooms,
          status,
          records_written,
          pages_scraped,
          run_id,
          completed_at
        FROM {params['bronze_runs_table']}
        ORDER BY completed_at DESC
        """
    )
)
