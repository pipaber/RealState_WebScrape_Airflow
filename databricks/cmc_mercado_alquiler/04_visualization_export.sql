-- Contrato completo para visualizaciones: únicamente datos gobernados en Gold.
-- Ejecutar después de que el job Bronze -> Silver -> Gold termine correctamente.

WITH latest_date AS (
  SELECT MAX(ingest_date) AS ingest_date
  FROM g101_cmc_mercado_alquiler.gold.market_daily_by_district
)
SELECT
  m.ingest_date,
  m.district,
  CAST(m.bedrooms AS INT) AS bedrooms,
  m.currency,
  CAST(m.listing_count AS BIGINT) AS listing_count,
  ROUND(m.avg_price, 2) AS avg_price,
  ROUND(m.median_price, 2) AS median_price,
  ROUND(m.min_price, 2) AS min_price,
  ROUND(m.max_price, 2) AS max_price,
  ROUND(m.avg_area_m2, 2) AS avg_area_m2,
  ROUND(m.median_price_per_m2, 2) AS median_price_per_m2,
  CAST(m.new_listing_count AS BIGINT) AS new_listing_count,
  CAST(m.removed_listing_count AS BIGINT) AS removed_listing_count,
  CAST(m.price_changed_count AS BIGINT) AS price_changed_count,
  ROUND(m.offer_change_pct, 2) AS offer_change_pct,
  m.calculated_at
FROM g101_cmc_mercado_alquiler.gold.market_daily_by_district AS m
INNER JOIN latest_date AS d
  ON m.ingest_date = d.ingest_date
WHERE m.currency = 'PEN'
  AND m.bedrooms BETWEEN 1 AND 4
ORDER BY
  m.district,
  m.bedrooms;
