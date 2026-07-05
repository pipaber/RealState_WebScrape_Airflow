# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Lima rental market by district and day
# MAGIC Aggregates silver into the metrics the project brief calls out: price
# MAGIC levels, price per m², and supply by district, trackable over
# MAGIC `ingest_date`. Recomputed in full from silver on every run.

# COMMAND ----------

# MAGIC %run ./config/parameters

# COMMAND ----------

from pyspark.sql import functions as F

catalog = params['catalog']
schema = params['schema']
silver_fqn = f"{catalog}.{schema}.{params['silver_table']}"
gold_fqn = f"{catalog}.{schema}.{params['gold_table']}"

silver_df = spark.table(silver_fqn)

# COMMAND ----------

gold_df = (
    silver_df
    .groupBy("district_clean", "ingest_date")
    .agg(
        F.count("*").alias("listing_count"),
        F.round(F.avg("price_amount"), 2).alias("avg_price"),
        F.round(F.min("price_amount"), 2).alias("min_price"),
        F.round(F.max("price_amount"), 2).alias("max_price"),
        F.round(F.avg("area_avg_m2"), 2).alias("avg_area_m2"),
        F.round(F.avg("price_per_m2"), 2).alias("avg_price_per_m2"),
        F.round(F.avg((F.col("bedrooms_min") + F.col("bedrooms_max")) / 2), 2).alias("avg_bedrooms"),
    )
    .withColumnRenamed("district_clean", "district")
    .orderBy("ingest_date", "district")
)

display(gold_df)

# COMMAND ----------

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_fqn)
)
print(f"Wrote {gold_df.count()} rows to {gold_fqn}.")
