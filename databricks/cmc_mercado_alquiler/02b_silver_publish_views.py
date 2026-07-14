# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: vista de opciones de dormitorios
# MAGIC
# MAGIC No es una vista materializada. Esta vista reutiliza el snapshot canónico
# MAGIC y expande únicamente los rangos 1-4 de dormitorios cuando un proyecto
# MAGIC ofrece varias configuraciones. No duplica almacenamiento ni requiere
# MAGIC refresh: siempre refleja `silver.listings_snapshot`.

# COMMAND ----------

# MAGIC %run ./config/parameters

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {params['silver_bedroom_options_view']}
    COMMENT 'Opciones de dormitorios derivadas del snapshot canónico de Urbania.'
    AS
    SELECT
      ingest_date,
      listing_id,
      explode(sequence(bedrooms_min, bedrooms_max)) AS bedrooms,
      listing_type,
      url,
      district,
      city,
      bathrooms,
      area_min_m2,
      area_max_m2,
      area_avg_m2,
      currency,
      price_amount,
      price_per_m2,
      features,
      publisher,
      run_id,
      source_file,
      source_observation_key,
      observed_at,
      updated_at
    FROM {params['silver_snapshot_table']}
    WHERE bedrooms_min BETWEEN 1 AND 4
      AND bedrooms_max BETWEEN bedrooms_min AND 4
    """
)

print(f"Vista publicada: {params['silver_bedroom_options_view']}")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT ingest_date, bedrooms, COUNT(DISTINCT listing_id) AS listing_count
        FROM {params['silver_bedroom_options_view']}
        GROUP BY ingest_date, bedrooms
        ORDER BY ingest_date DESC, bedrooms
        """
    )
)
