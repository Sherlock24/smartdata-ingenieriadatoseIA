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

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cat_ftr_smartdata_dev.golden.ventas_diarias_tienda (
# MAGIC   TIENDA                    STRING,
# MAGIC   FECHA                     DATE,
# MAGIC   NUM_PEDIDOS               INT,
# MAGIC   TOTAL_UNIDADES            INT,
# MAGIC   INGRESO_BRUTO             DOUBLE,
# MAGIC   INGRESO_NETO              DOUBLE,
# MAGIC   DESCUENTO_MEDIO_PCT       DOUBLE,
# MAGIC   NUM_LINEAS_CON_DESCUENTO  INT,
# MAGIC   NUM_LINEAS_TOTAL          INT,
# MAGIC   PCT_LINEAS_CON_DESCUENTO  DOUBLE,
# MAGIC   FECHA_ACTUALIZACION       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://gold@${storageName}.dfs.core.windows.net/ventas_diarias_tienda"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cat_ftr_smartdata_dev.golden.ventas_categoria_mes (
# MAGIC   CATEGORIA                 STRING,
# MAGIC   ANIO                      INT,
# MAGIC   MES                       INT,
# MAGIC   NUM_PEDIDOS               INT,
# MAGIC   NUM_ARTICULOS             INT,
# MAGIC   TOTAL_UNIDADES            INT,
# MAGIC   NUM_LINEAS_TOTAL          INT,
# MAGIC   INGRESO_BRUTO             DOUBLE,
# MAGIC   INGRESO_NETO              DOUBLE,
# MAGIC   TICKET_PROMEDIO           DOUBLE,
# MAGIC   PRECIO_PROMEDIO           DOUBLE,
# MAGIC   NUM_LINEAS_CON_DESCUENTO  INT,
# MAGIC   PCT_LINEAS_CON_DESCUENTO  DOUBLE,
# MAGIC   DESCUENTO_MEDIO_PCT       DOUBLE,
# MAGIC   NUM_PRECIO_NORMAL         INT,
# MAGIC   NUM_PROMOCION             INT,
# MAGIC   NUM_BONO_EMPRESARIAL      INT,
# MAGIC   NUM_REGALO                INT,
# MAGIC   FECHA_ACTUALIZACION       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://gold@${storageName}.dfs.core.windows.net/ventas_categoria_mes"
# MAGIC