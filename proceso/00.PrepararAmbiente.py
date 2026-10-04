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

# DBTITLE 1,Cell 21
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cat_ftr_smartdata_dev.bronze.ventas (
# MAGIC ID_ORDER STRING,
# MAGIC STORE STRING,
# MAGIC REG INT,
# MAGIC DATE DATE,
# MAGIC ID_ITEM STRING,
# MAGIC QTY INT,
# MAGIC PRICE DECIMAL(12,2),
# MAGIC DISCOUNT DECIMAL(12,2),
# MAGIC ID_SELLER STRING,
# MAGIC STATUS STRING,
# MAGIC INGESTION_DATE TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@${storageName}.dfs.core.windows.net/ventas"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cat_ftr_smartdata_dev.bronze.productos (
# MAGIC ID_ITEM STRING,
# MAGIC PRODUCT STRING,
# MAGIC CATEGORY STRING,
# MAGIC INGESTION_DATE TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@${storageName}.dfs.core.windows.net/productos"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABLAS SILVER

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cat_ftr_smartdata_dev.silver.ventas_productos_categorias (
# MAGIC   ID_ARTICULO STRING,
# MAGIC   PRODUCTO STRING,
# MAGIC   CATEGORIA STRING,
# MAGIC   ID_PEDIDO STRING,
# MAGIC   TIENDA STRING,
# MAGIC   NUM_LINEA INT,
# MAGIC   FECHA DATE,
# MAGIC   CANTIDAD INT,
# MAGIC   PRECIO DOUBLE,
# MAGIC   DESCUENTO INT,
# MAGIC   ID_VENDEDOR STRING,
# MAGIC   ETIQUETA_DESCUENTO STRING
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