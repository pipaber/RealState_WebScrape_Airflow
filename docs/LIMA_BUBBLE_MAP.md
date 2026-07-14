# Mapa coroplético de alquileres de Lima

El gráfico usa `GeoPandas` y un **Shapefile de límites distritales**. La composición tiene tres lecturas complementarias:

- mapa coroplético: nivel de alquiler por distrito;
- matriz comercial: cantidad de avisos frente al alquiler promedio;
- ranking de oferta: distritos con mayor inventario y su ticket promedio.

El cuadrante superior derecho de la matriz identifica territorios con oferta y ticket altos, una primera señal para priorizar la fuerza de ventas. El CSV debe generarse con `databricks/cmc_mercado_alquiler/04_visualization_export.sql` y conservar:

```text
ingest_date, district, bedrooms, currency, listing_count, avg_price,
median_price, min_price, max_price, avg_area_m2, median_price_per_m2,
new_listing_count, removed_listing_count, price_changed_count,
offer_change_pct, calculated_at
```

Los generadores rechazan el CSV anterior y exigen este contrato completo de Gold.

## 1. Descargar los límites

Usa la capa oficial **Límites Distritales** del [Geoportal IDEP / IGN](https://portalgeo.idep.gob.pe/). El geoportal publica la capa a escala 1:100 000 y permite la descarga de datos geoespaciales.

El dataset descargado para este proyecto está en `data/reference/distritos.rar`. Ya fue extraído a:

```text
data/reference/distritos/DISTRITOS.shp
```

GeoPandas puede leer un `.zip` o un `.shp`, pero no un `.rar` directamente. Por eso el script debe recibir el `.shp` extraído.

## 2. Instalar dependencias y generar el PNG

```powershell
uv sync
uv run python scripts/plot_lima_bubble_map.py `
  scripts/gold_visualization_full_export.csv `
  --boundaries data/reference/distritos/DISTRITOS.shp
```

El resultado se escribe en `reports/lima_rental_choropleth.png`.

Para generar la versión interactiva en D3.js con los mismos datos:

```powershell
uv run python scripts/build_lima_dashboard_d3.py <csv_exportado> `
  --boundaries data/reference/distritos/DISTRITOS.shp
```

El resultado se escribe en `reports/lima_rental_dashboard_d3.html`. El archivo
es autocontenido respecto a datos y geometrías; solo carga D3.js desde CDN al
abrirse. Al pasar el cursor por un distrito se resaltan el mapa, la matriz y el
ranking de forma coordinada.

La matriz complementaria compara espacio, alquiler mediano, precio por m² y
profundidad de mercado en cuatro paneles con escala compartida:

```powershell
uv run python scripts/plot_lima_value_matrix.py `
  scripts/gold_visualization_full_export.csv `
  --boundaries data/reference/distritos/DISTRITOS.shp
```

El resultado se escribe en `reports/lima_rental_value_matrix.png`.

La versión interactiva equivalente se genera con:

```powershell
uv run python scripts/build_lima_value_matrix_d3.py `
  scripts/gold_visualization_full_export.csv `
  --boundaries data/reference/distritos/DISTRITOS.shp
```

El resultado se escribe en `reports/lima_rental_value_matrix_d3.html`.

Por defecto, los rangos son `<1,500`, `1,500–1,999`, `2,000–2,499`, `2,500–2,999`, `3,000–3,999` y `>=4,000` PEN. Puedes definir otros puntos de corte:

```powershell
uv run python scripts/plot_lima_bubble_map.py <csv> `
  --boundaries data/reference/distritos/DISTRITOS.shp `
  --bins 1200 1800 2400 3000 4000
```

La capa se limita automáticamente a Lima y Callao cuando el Shapefile contiene un campo de departamento. El script busca columnas usuales como `NOMBDIST` o `DISTRITO`; si la tuya usa otra, indícala:

```powershell
uv run python scripts/plot_lima_bubble_map.py <csv> `
  --boundaries <archivo.zip> `
  --district-column <nombre_de_columna>
```
