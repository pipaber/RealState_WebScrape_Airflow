# Configuración del Job Medallion en Databricks

Después de importar los notebooks, crea un Job llamado:

```text
g101_cmc_mercado_alquiler_daily
```

Usa un compute compatible con Unity Catalog. Para la primera ejecución puede
ser el mismo compute que se utilizó en Bronze.

| Orden | Nombre de tarea | Notebook | Dependencia | Parámetro inicial |
|---:|---|---|---|---|
| 1 | `bronze_ingest` | `01_bronze_ingest` | Ninguna | Ninguno |
| 2 | `silver_build_snapshot` | `02_silver_build_snapshot` | `bronze_ingest` | `processing_date`: vacío |
| 3 | `silver_publish_views` | `02b_silver_publish_views` | `silver_build_snapshot` | Ninguno |
| 4 | `validate_silver` | `02c_validate_silver` | `silver_publish_views` | `processing_date`: vacío |
| 5 | `gold_build_outputs` | `03_gold_build_outputs` | `validate_silver` | `processing_date`: vacío |
| 6 | `validate_gold` | `03b_validate_gold` | `gold_build_outputs` | `processing_date`: vacío |

Un valor vacío de `processing_date` toma automáticamente la fecha más reciente
que tenga manifests `success` en Bronze. Para reprocessar una fecha específica,
asigna `YYYY-MM-DD` a las tareas 2, 4, 5 y 6.

El Job se ejecuta después de que Airflow termine de subir los archivos Raw a
ADLS. No agregues `00_validate_infrastructure` al Job: es una verificación
manual de infraestructura, no una transformación diaria.

Antes de programarlo diariamente, ejecútalo una vez de forma manual y verifica
que las tareas `validate_silver` y `validate_gold` terminen en estado
**Succeeded**.
