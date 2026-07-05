# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: land raw urbania listings into Delta
# MAGIC Reads the raw NDJSON file picked by `config/parameters`, tags it with
# MAGIC ingestion metadata, and appends it into the bronze Delta table —
# MAGIC deduplicated by `_run_id` so re-running this notebook on the same file
# MAGIC is a no-op.

# COMMAND ----------

# MAGIC %run ./config/parameters

# COMMAND ----------

from pyspark.sql import functions as F

catalog = params['catalog']
schema = params['schema']
bronze_table = params['bronze_table']
bronze_fqn = f"{catalog}.{schema}.{bronze_table}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# COMMAND ----------

raw_df = spark.read.json(params['source_path'])

bronze_df = (
    raw_df
    .withColumn("_bronze_source_file", F.col("_metadata.file_path"))
    .withColumn("_bronze_loaded_at", F.current_timestamp())
    # ingest_date derived from the scrape timestamp rather than the partition
    # folder name, so it survives reading a single file path directly.
    .withColumn("ingest_date", F.to_date("_ingested_at"))
)

print(f"Read {bronze_df.count()} rows from {params['source_path']}")
display(bronze_df)

# COMMAND ----------

if not spark.catalog.tableExists(bronze_fqn):
    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_fqn)
    )
    print(f"Created {bronze_fqn} with {bronze_df.count()} rows.")
else:
    existing_run_ids = spark.table(bronze_fqn).select("_run_id").distinct()
    new_rows_df = bronze_df.join(existing_run_ids, on="_run_id", how="left_anti")
    new_count = new_rows_df.count()
    if new_count == 0:
        print(f"run_id already present in {bronze_fqn}; nothing to append.")
    else:
        (
            new_rows_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(bronze_fqn)
        )
        print(f"Appended {new_count} new rows to {bronze_fqn}.")

# COMMAND ----------

display(spark.table(bronze_fqn))
