# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: limpiar y tipar los listings de urbania
# MAGIC Bronze conserva cada campo como el string crudo que muestra el sitio
# MAGIC (`"S/ 2,750"`, `"45 a 60 m²"`, `"1 a 2 dorm."`). Silver los convierte en
# MAGIC columnas numéricas. Se recalcula por completo desde bronze en cada
# MAGIC corrida — bronze es la fuente de verdad y esta tabla todavía es
# MAGIC pequeña, no hace falta lógica de merge incremental.

# COMMAND ----------

# MAGIC %run ./config/parameters

# COMMAND ----------

from pyspark.sql import functions as F

catalog = params['catalog']
bronze_fqn = f"{catalog}.{params['bronze_schema']}.{params['bronze_table']}"
silver_fqn = f"{catalog}.{params['silver_schema']}.{params['silver_table']}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{params['silver_schema']}`")

bronze_df = spark.table(bronze_fqn)


def _numerico(raw_col, pattern):
    """Extrae la primera coincidencia numérica, quita las comas de miles y castea a double."""
    extraido = F.regexp_extract(raw_col, pattern, 1)
    return F.when(extraido == "", None).otherwise(
        F.regexp_replace(extraido, ",", "").cast("double")
    )


NUMERO = r"(\d+(?:[.,]\d+)*)"

# COMMAND ----------

silver_df = (
    bronze_df
    # --- precio: "S/ 2,750" / "Departamentos desde S/ 2,750" / "US$ 1,200" ---
    .withColumn(
        "currency_clean",
        F.when(F.col("price_raw").rlike(r"US\$"), F.lit("USD"))
         .when(F.col("price_raw").rlike(r"S/\.?"), F.lit("PEN"))
         .otherwise(F.coalesce(F.col("currency"), F.lit("PEN"))),
    )
    .withColumn("price_amount", F.coalesce(F.col("price_min"), _numerico(F.col("price_raw"), NUMERO)))
    .withColumn("maintenance_amount", _numerico(F.col("maintenance_raw"), NUMERO))
    # --- rangos: "45 a 60 m²" / "1 a 2 dorm." — el máximo cae al mínimo si no hay rango ---
    .withColumn("area_min_m2", _numerico(F.col("area_raw"), NUMERO))
    .withColumn(
        "area_max_m2",
        F.coalesce(_numerico(F.col("area_raw"), r"a\s*" + NUMERO), F.col("area_min_m2")),
    )
    .withColumn("bedrooms_min", _numerico(F.col("bedrooms_raw"), NUMERO).cast("int"))
    .withColumn(
        "bedrooms_max",
        F.coalesce(_numerico(F.col("bedrooms_raw"), r"a\s*" + NUMERO), F.col("bedrooms_min").cast("double")).cast("int"),
    )
    .withColumn("bathrooms_min", _numerico(F.col("bathrooms_raw"), NUMERO).cast("int"))
    .withColumn(
        "bathrooms_max",
        F.coalesce(_numerico(F.col("bathrooms_raw"), r"a\s*" + NUMERO), F.col("bathrooms_min").cast("double")).cast("int"),
    )
    .withColumn("units", _numerico(F.col("units_raw"), NUMERO).cast("int"))
    # --- ubicación / timestamps ---
    .withColumn("district_clean", F.trim(F.initcap(F.col("district"))))
    .withColumn("city_clean", F.trim(F.initcap(F.col("city"))))
    .withColumn("ingested_at_ts", F.to_timestamp("_ingested_at"))
    .withColumn("area_avg_m2", (F.col("area_min_m2") + F.col("area_max_m2")) / 2)
    .withColumn(
        "price_per_m2",
        F.when(F.col("area_avg_m2") > 0, F.col("price_amount") / F.col("area_avg_m2")),
    )
)

display(silver_df)

# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_fqn)
)
print(f"Se escribieron {silver_df.count()} filas en {silver_fqn}.")
