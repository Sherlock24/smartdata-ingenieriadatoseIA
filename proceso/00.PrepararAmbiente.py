# Databricks notebook source
# MAGIC %md
# MAGIC ## Preparación de Ambiente

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text storageName default "adlsftrsmardatadev01";

# COMMAND ----------

storageName = dbutils.widgets.get("storageName")

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXTERNAL LOCATIONS
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `extloc-ftr-smartdata-adlsmetastore-dev-01`
# MAGIC URL 'abfss://metastore@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `cred-ftr-smartdata-azure-dev-01`)
# MAGIC COMMENT 'Ubicación externa para las tablas de unit-catalog del data lake de retail';

# COMMAND ----------

# DBTITLE 1,Cell 10
# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `extloc-ftr-smartdata-adlsraw-dev-01`
# MAGIC URL 'abfss://raw@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `cred-ftr-smartdata-azure-dev-01`)
# MAGIC COMMENT 'Ubicación externa para las tablas raw del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `extloc-ftr-smartdata-adlsbronze-dev-01`
# MAGIC URL 'abfss://bronze@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `cred-ftr-smartdata-azure-dev-01`)
# MAGIC COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `extloc-ftr-smartdata-adlssilver-dev-01`
# MAGIC URL 'abfss://silver@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `cred-ftr-smartdata-azure-dev-01`)
# MAGIC COMMENT 'Ubicación externa para las tablas silver del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `extloc-ftr-smartdata-adlsgold-dev-01`
# MAGIC URL 'abfss://gold@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `cred-ftr-smartdata-azure-dev-01`)
# MAGIC COMMENT 'Ubicación externa para las tablas gold del Data Lake';

# COMMAND ----------

# MAGIC %md
# MAGIC ## CATÁLOGO

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS cat_ftr_smartdata_dev CASCADE;
# MAGIC      

# COMMAND ----------

# DBTITLE 1,Cell 16
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS cat_ftr_smartdata_dev
# MAGIC MANAGED LOCATION 'abfss://metastore@${storageName}.dfs.core.windows.net/'
# MAGIC COMMENT 'Catalogo para la arquitectura medallion del ambiente de dev';

# COMMAND ----------

# MAGIC %md
# MAGIC ## ESQUEMAS

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS cat_ftr_smartdata_dev.raw;
# MAGIC DROP SCHEMA IF EXISTS cat_ftr_smartdata_dev.bronze;
# MAGIC DROP SCHEMA IF EXISTS cat_ftr_smartdata_dev.silver;
# MAGIC DROP SCHEMA IF EXISTS cat_ftr_smartdata_dev.golden;

# COMMAND ----------

dbutils.fs.rm(f"abfss://bronze@{storageName}.dfs.core.windows.net/",True)
dbutils.fs.rm(f"abfss://silver@{storageName}.dfs.core.windows.net/",True)
dbutils.fs.rm(f"abfss://gold@{storageName}.dfs.core.windows.net/",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS cat_ftr_smartdata_dev.raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS cat_ftr_smartdata_dev.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS cat_ftr_smartdata_dev.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS cat_ftr_smartdata_dev.golden;

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABLAS BRONZE

# COMMAND ----------

# DBTITLE 1,Cell 19 - Tabla bronze.ventas
# MAGIC %sql
# MAGIC -- Tabla Delta bronze para ventas (orders.csv)
# MAGIC -- Campos de negocio + campos de auditoría mínimos: INGESTION_DATE, SOURCE_FILE_NAME, INGESTION_USER
# MAGIC CREATE TABLE IF NOT EXISTS cat_ftr_smartdata_dev.bronze.ventas (
# MAGIC ID_ORDER         STRING,
# MAGIC STORE            STRING,
# MAGIC REG              INT,
# MAGIC DATE             DATE,
# MAGIC ID_ITEM          STRING,
# MAGIC QTY              INT,
# MAGIC PRICE            DECIMAL(12,2),
# MAGIC DISCOUNT         DECIMAL(12,2),
# MAGIC ID_SELLER        STRING,
# MAGIC STATUS           STRING,
# MAGIC INGESTION_DATE   TIMESTAMP,
# MAGIC SOURCE_FILE_NAME STRING,
# MAGIC INGESTION_USER   STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@${storageName}.dfs.core.windows.net/ventas"
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 20 - Tabla bronze.productos
# MAGIC %sql
# MAGIC -- Tabla Delta bronze para productos (items.csv)
# MAGIC -- Campos de negocio + campos de auditoría mínimos: INGESTION_DATE, SOURCE_FILE_NAME, INGESTION_USER
# MAGIC CREATE TABLE IF NOT EXISTS cat_ftr_smartdata_dev.bronze.productos (
# MAGIC ID_ITEM          STRING,
# MAGIC PRODUCT          STRING,
# MAGIC CATEGORY         STRING,
# MAGIC INGESTION_DATE   TIMESTAMP,
# MAGIC SOURCE_FILE_NAME STRING,
# MAGIC INGESTION_USER   STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@${storageName}.dfs.core.windows.net/productos"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABLAS SILVER

# COMMAND ----------

# DBTITLE 1,Tabla silver.ventas_productos_categorias
# MAGIC %sql
# MAGIC -- Tabla Delta Silver: join enriquecido de ventas (STATUS='VALID') + productos
# MAGIC -- Incluye métricas financieras, dimensiones temporales, segmentación y auditoría Silver
# MAGIC CREATE TABLE IF NOT EXISTS cat_ftr_smartdata_dev.silver.ventas_productos_categorias (
# MAGIC   ID_ARTICULO        STRING     COMMENT 'Identificador del artículo',
# MAGIC   PRODUCTO           STRING     COMMENT 'Nombre del producto (normalizado)',
# MAGIC   CATEGORIA          STRING     COMMENT 'Categoría del producto (normalizada)',
# MAGIC   ID_PEDIDO          STRING     COMMENT 'Identificador del pedido',
# MAGIC   TIENDA             STRING     COMMENT 'Nombre de la tienda (normalizado)',
# MAGIC   NUM_LINEA          STRING     COMMENT 'Número de línea dentro del pedido',
# MAGIC   FECHA              DATE       COMMENT 'Fecha de la venta',
# MAGIC   CANTIDAD           INT        COMMENT 'Unidades vendidas',
# MAGIC   PRECIO             DOUBLE     COMMENT 'Precio unitario',
# MAGIC   DESCUENTO          INT        COMMENT 'Porcentaje de descuento aplicado',
# MAGIC   ID_VENDEDOR        STRING     COMMENT 'Identificador del vendedor',
# MAGIC   ETIQUETA_DESCUENTO STRING     COMMENT 'Categoría: Precio Normal / Promoción / Bono Empresarial / Regalo',
# MAGIC   IMPORTE_BRUTO      DOUBLE     COMMENT 'CANTIDAD x PRECIO',
# MAGIC   IMPORTE_DESCUENTO  DOUBLE     COMMENT 'Monto del descuento aplicado',
# MAGIC   IMPORTE_NETO       DOUBLE     COMMENT 'IMPORTE_BRUTO menos IMPORTE_DESCUENTO',
# MAGIC   ANIO               INT        COMMENT 'Año de la venta',
# MAGIC   MES                INT        COMMENT 'Mes de la venta',
# MAGIC   TRIMESTRE          INT        COMMENT 'Trimestre de la venta (1-4)',
# MAGIC   DIA_SEMANA         INT        COMMENT 'Día de la semana: 1=Dom, 7=Sáb',
# MAGIC   ES_FIN_SEMANA      BOOLEAN    COMMENT 'True si la venta ocurrió en sábado o domingo',
# MAGIC   RANGO_PRECIO       STRING     COMMENT 'Segmento: Económico / Estándar / Premium / Lujo',
# MAGIC   FECHA_PROCESO      TIMESTAMP  COMMENT 'Fecha y hora de la transformación Silver',
# MAGIC   INGESTION_USER     STRING     COMMENT 'Usuario o SP que ejecutó el proceso Silver'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@${storageName}.dfs.core.windows.net/ventas_productos_categorias"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Tablas Gold

# COMMAND ----------

# DBTITLE 1,Tabla golden.ventas_diarias_tienda
# MAGIC %sql
# MAGIC -- Tabla Gold: KPIs diarios por tienda para Power BI
# MAGIC -- ANIO/MES/TRIMESTRE permiten filtros de tiempo directos sin tabla de fechas adicional
# MAGIC CREATE TABLE IF NOT EXISTS cat_ftr_smartdata_dev.golden.ventas_diarias_tienda (
# MAGIC   TIENDA                    STRING   COMMENT 'Nombre de la tienda',
# MAGIC   FECHA                     DATE     COMMENT 'Fecha de la venta',
# MAGIC   ANIO                      INT      COMMENT 'Año (para filtros en Power BI)',
# MAGIC   MES                       INT      COMMENT 'Mes (para filtros en Power BI)',
# MAGIC   TRIMESTRE                 INT      COMMENT 'Trimestre 1-4 (para análisis trimestral)',
# MAGIC   NUM_PEDIDOS               INT      COMMENT 'Pedidos distintos del día',
# MAGIC   TOTAL_UNIDADES            INT      COMMENT 'Unidades vendidas',
# MAGIC   NUM_LINEAS_TOTAL          INT      COMMENT 'Líneas de pedido totales',
# MAGIC   INGRESO_BRUTO             DOUBLE   COMMENT 'Suma de IMPORTE_BRUTO (CANTIDAD x PRECIO)',
# MAGIC   INGRESO_NETO              DOUBLE   COMMENT 'Suma de IMPORTE_NETO (después de descuento)',
# MAGIC   INGRESO_DESCUENTO         DOUBLE   COMMENT 'Monto total de descuento aplicado',
# MAGIC   DESCUENTO_MEDIO_PCT       DOUBLE   COMMENT 'Descuento promedio en líneas con descuento',
# MAGIC   NUM_LINEAS_CON_DESCUENTO  INT      COMMENT 'Líneas con descuento > 0',
# MAGIC   PCT_LINEAS_CON_DESCUENTO  DOUBLE   COMMENT 'Porcentaje de líneas con descuento',
# MAGIC   TICKET_PROMEDIO           DOUBLE    COMMENT 'Ingreso neto / número de pedidos',
# MAGIC   FECHA_ACTUALIZACION       TIMESTAMP COMMENT 'Cuándo se calculó la agregación Gold',
# MAGIC   INGESTION_USER            STRING    COMMENT 'Usuario o SP que ejecutó el pipeline Gold'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://gold@${storageName}.dfs.core.windows.net/ventas_diarias_tienda"
# MAGIC

# COMMAND ----------

# DBTITLE 1,Tabla golden.ventas_categoria_mes
# MAGIC %sql
# MAGIC -- Tabla Gold: KPIs mensuales por categoría con mix de tipo de venta para Power BI
# MAGIC CREATE TABLE IF NOT EXISTS cat_ftr_smartdata_dev.golden.ventas_categoria_mes (
# MAGIC   CATEGORIA                 STRING   COMMENT 'Categoría de producto',
# MAGIC   ANIO                      INT      COMMENT 'Año',
# MAGIC   MES                       INT      COMMENT 'Mes',
# MAGIC   TRIMESTRE                 INT      COMMENT 'Trimestre 1-4',
# MAGIC   NUM_PEDIDOS               INT      COMMENT 'Pedidos distintos del mes',
# MAGIC   NUM_ARTICULOS             INT      COMMENT 'Artículos distintos vendidos',
# MAGIC   TOTAL_UNIDADES            INT      COMMENT 'Unidades vendidas',
# MAGIC   NUM_LINEAS_TOTAL          INT      COMMENT 'Líneas de pedido totales',
# MAGIC   INGRESO_BRUTO             DOUBLE   COMMENT 'Suma de IMPORTE_BRUTO',
# MAGIC   INGRESO_NETO              DOUBLE   COMMENT 'Suma de IMPORTE_NETO',
# MAGIC   TICKET_PROMEDIO           DOUBLE   COMMENT 'Ingreso neto / número de pedidos',
# MAGIC   PRECIO_PROMEDIO           DOUBLE   COMMENT 'Precio unitario promedio',
# MAGIC   NUM_LINEAS_CON_DESCUENTO  INT      COMMENT 'Líneas con descuento > 0',
# MAGIC   PCT_LINEAS_CON_DESCUENTO  DOUBLE   COMMENT 'Porcentaje de líneas con descuento',
# MAGIC   DESCUENTO_MEDIO_PCT       DOUBLE   COMMENT 'Descuento promedio en líneas con descuento',
# MAGIC   NUM_PRECIO_NORMAL         INT      COMMENT 'Líneas con etiqueta Precio Normal',
# MAGIC   NUM_PROMOCION             INT      COMMENT 'Líneas con etiqueta Promoción',
# MAGIC   NUM_BONO_EMPRESARIAL      INT      COMMENT 'Líneas con etiqueta Bono Empresarial',
# MAGIC   NUM_REGALO                INT       COMMENT 'Líneas con etiqueta Regalo',
# MAGIC   FECHA_ACTUALIZACION       TIMESTAMP COMMENT 'Cuándo se calculó la agregación Gold',
# MAGIC   INGESTION_USER            STRING    COMMENT 'Usuario o SP que ejecutó el pipeline Gold'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://gold@${storageName}.dfs.core.windows.net/ventas_categoria_mes"
# MAGIC

# COMMAND ----------

# DBTITLE 1,Tabla golden.kpi_vendedor_mes
# MAGIC %sql
# MAGIC -- Tabla Gold: KPIs mensuales por vendedor y tienda para análisis de rendimiento comercial en Power BI
# MAGIC CREATE TABLE IF NOT EXISTS cat_ftr_smartdata_dev.golden.kpi_vendedor_mes (
# MAGIC   ID_VENDEDOR               STRING   COMMENT 'Identificador del vendedor',
# MAGIC   TIENDA                    STRING   COMMENT 'Tienda a la que pertenece el vendedor',
# MAGIC   ANIO                      INT      COMMENT 'Año',
# MAGIC   MES                       INT      COMMENT 'Mes',
# MAGIC   TRIMESTRE                 INT      COMMENT 'Trimestre 1-4',
# MAGIC   NUM_PEDIDOS               INT      COMMENT 'Pedidos distintos gestionados',
# MAGIC   NUM_ARTICULOS_DISTINTOS   INT      COMMENT 'Artículos distintos vendidos',
# MAGIC   TOTAL_UNIDADES            INT      COMMENT 'Unidades vendidas',
# MAGIC   NUM_LINEAS                INT      COMMENT 'Líneas de pedido totales',
# MAGIC   INGRESO_BRUTO             DOUBLE   COMMENT 'Suma de IMPORTE_BRUTO',
# MAGIC   INGRESO_NETO              DOUBLE   COMMENT 'Suma de IMPORTE_NETO',
# MAGIC   INGRESO_DESCUENTO         DOUBLE   COMMENT 'Monto total de descuento aplicado',
# MAGIC   DESCUENTO_MEDIO_PCT       DOUBLE   COMMENT 'Descuento promedio en líneas con descuento',
# MAGIC   NUM_LINEAS_CON_DESCUENTO  INT      COMMENT 'Líneas con descuento > 0',
# MAGIC   PCT_LINEAS_CON_DESCUENTO  DOUBLE   COMMENT 'Porcentaje de líneas con descuento',
# MAGIC   TICKET_PROMEDIO           DOUBLE   COMMENT 'Ingreso neto / número de pedidos',
# MAGIC   NUM_PRECIO_NORMAL         INT      COMMENT 'Líneas con etiqueta Precio Normal',
# MAGIC   NUM_PROMOCION             INT      COMMENT 'Líneas con etiqueta Promoción',
# MAGIC   NUM_BONO_EMPRESARIAL      INT      COMMENT 'Líneas con etiqueta Bono Empresarial',
# MAGIC   NUM_REGALO                INT       COMMENT 'Líneas con etiqueta Regalo',
# MAGIC   FECHA_ACTUALIZACION       TIMESTAMP COMMENT 'Cuándo se calculó la agregación Gold',
# MAGIC   INGESTION_USER            STRING    COMMENT 'Usuario o SP que ejecutó el pipeline Gold'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://gold@${storageName}.dfs.core.windows.net/kpi_vendedor_mes"