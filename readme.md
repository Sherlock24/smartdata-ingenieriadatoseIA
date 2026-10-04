# Resumen del Proyecto - Pipeline de Ventas Retail con Arquitectura Medallion

Este proyecto consiste en un pipeline completo de ingeniería de datos construido sobre Azure Databricks, que procesa información de ventas y productos de una cadena retail siguiendo una arquitectura **Medallion (Bronze → Silver → Golden)**.

El objetivo es tomar los datos crudos de pedidos y artículos, limpiarlos, unirlos y transformarlos hasta llegar a tablas analíticas listas para el reporting (descuentos aplicados, ingresos por tienda y por categoría, etc.).

![Contenedores](evidencias/Contenedores.PNG)

---

## Características principales

- Arquitectura Medallion completa (Bronze → Silver → Golden) sobre Unity Catalog
- Ingesta parametrizada con `dbutils.widgets` (catálogo, esquemas, nombre del storage) — sin valores fijos en el código
- Gobernanza de datos mediante External Locations y Storage Credentials de Unity Catalog
- Reglas de negocio aplicadas con Spark `when` (etiquetado de descuentos) en lugar de UDFs, por rendimiento
- Agregaciones Golden listas para análisis: ventas diarias por tienda y ventas mensuales por categoría
- Pipeline orquestado como job de Databricks Workflows con dependencias entre tareas
- CI/CD con GitHub Actions que exporta, despliega y ejecuta automáticamente el pipeline en producción
- Notebook de reversión (`reversion/reverso.ipynb`) para limpiar tablas y datos del esquema completo
- Visualización de resultados en Power BI sobre las tablas Golden

---

## ¿De qué trata el proyecto?

El proyecto parte de dos archivos CSV crudos — `items.csv` (catálogo de productos) y `orders.csv` (pedidos de venta) — y construye un pipeline que los limpia, filtra, une y agrega hasta obtener métricas de negocio: ingresos brutos y netos, unidades vendidas, porcentaje de líneas con descuento, ticket promedio, etc., agrupadas por tienda/día y por categoría/mes.

Para esto fue necesario configurar un Data Lake en Azure (ADLS Gen2) con un contenedor por capa, gobernar el acceso mediante Unity Catalog (External Locations y Storage Credentials), parametrizar todos los notebooks con widgets (catálogo, esquemas, nombre del storage), y automatizar el despliegue a producción mediante GitHub Actions.

---

## Stack tecnológico

- **Azure Databricks** — entorno de ejecución de los notebooks y jobs
- **PySpark** — procesamiento y transformación de datos
- **Delta Lake** — formato de almacenamiento de las tablas
- **Unity Catalog** — gobernanza, catálogo (`catalog_dev`) y control de acceso (External Locations, Storage Credentials, Grants)
- **Azure Data Lake Storage Gen2** — almacenamiento de archivos por capa (raw, bronze, silver, golden)
- **GitHub Actions** — CI/CD para el despliegue automático de notebooks y jobs a producción
- **Power BI** — visualización y dashboards sobre las tablas Golden

---

## Arquitectura (Medallion)

![Workflow](evidencias/Databricks/WorkfLow.png)

Los datos se separan en contenedores distintos por capa dentro de ADLS Gen2 (`raw`, `bronze`, `silver`, `golden`), cada uno gobernado mediante una **External Location** (`extl-raw`, `extl-bronze`, `extl-silver`, `extl-golden`, `extl-catalog`) respaldada por una **Storage Credential** llamada `credential`. Sobre esa base se organiza el catálogo `catalog_dev` con un esquema por capa:

| Esquema  | Contenedor ADLS | Contenido |
|----------|-----------------|-----------|
| `raw`    | `raw`           | Archivos CSV de origen (`orders.csv`, `items.csv`) |
| `bronze` | `bronze`        | Tablas Delta con los datos crudos ingeridos + columna `INGESTION_DATE` |
| `silver` | `silver`        | Tabla limpia y unida `ventas_productos_categorias` |
| `golden` | `golden`        | Tablas agregadas `ventas_diarias_tienda` y `ventas_categoria_mes` |

---

## Estructura del repositorio

```
smartdata-ingenieriadatoseIA/
├── proceso/                                    # Notebooks del pipeline (orden de ejecución)
│   ├── 00.PrepararAmbiente.ipynb                # Setup: external locations, catálogo, esquemas y tablas vacías
│   ├── 01.IngestaProductos.ipynb                # raw -> bronze.productos
│   ├── 01.IngestaVentas.ipynb                   # raw -> bronze.ventas
│   ├── 02.Transform_Ventas_Productos.ipynb      # bronze -> silver (join + etiqueta de descuento)
│   ├── 03.Transform_Gold_Ventas_Diarias_Tienda.ipynb   # silver -> golden (agregación diaria por tienda)
│   └── 03.Transform_Gold_Ventas_Categoria_Mes.ipynb    # silver -> golden (agregación mensual por categoría)
├── seguridad/
│   └── 01.Grants.ipynb                          # Permisos (GRANT/REVOKE) sobre catálogo, esquemas y external locations
├── reversion/
│   └── reverso.ipynb                           # Notebook para eliminar tablas y datos del esquema
├── datasets/                                   # CSVs de origen (items.csv, orders.csv)
├── dashboard/
│   └── Dashboard.pbix                          # Dashboard de Power BI sobre las tablas Golden
├── evidencias/                                 # Capturas de los recursos, workflows y dashboards
└── .github/workflows/
    └── 
# CI/CD: despliegue y ejecución del pipeline en producción
```

---

## Dataset utilizado

Los datos provienen de dos archivos planos cargados al contenedor `raw`:

| Archivo | Descripción | Filas aprox. |
|---------|-------------|--------------|
| `items.csv` | Catálogo de productos — ID, nombre y categoría | 39.194 |
| `orders.csv` | Pedidos de venta — tienda, fecha, artículo, cantidad, precio, descuento, vendedor y estado | 1.090.380 |

### 1. Recursos de Azure utilizados
- Storage Account con Hierarchical Namespace habilitado (ADLS Gen2)
- Contenedores: `raw`, `bronze`, `silver`, `golden`
- Azure Databricks Workspace con Unity Catalog habilitado
- External Locations (raw,bronze,silver,gold,metastore) y Storage Credential `cred-ftr-smartdata-azure-dev-01`
- Clusters `cl-ftr-smartdata-dev-01` (desarrollo)

### 2. Configuración del ambiente y Unity Catalog
El notebook `proceso/00.PrepararAmbiente.ipynb` se ejecuta una sola vez para crear las External Locations, el catálogo `cat_ftr_smartdata_dev`, los esquemas (`raw`, `bronze`, `silver`, `golden`) y las tablas Delta vacías. El notebook `seguridad/01.Grants.ipynb` documenta los permisos (GRANT/REVOKE) sobre catálogo, esquemas, tablas y External Locations.

### 3. Ingesta de los datasets
Los notebooks `proceso/01.IngestaProductos.ipynb` y `proceso/01IngestaVentas.ipynb` leen `items.csv` y `orders.csv` desde el contenedor `raw` y los escriben como tablas Delta en `bronze.productos` y `bronze.ventas`, agregando la columna `INGESTION_DATE`.

### 4. Pipeline de transformación
A partir de las tablas Bronze, el pipeline construye la capa Silver y luego las tablas Golden mediante los notebooks de la carpeta `proceso/`.

### 5. Job configurado
En Databricks Workflows se configuró el job `job-ftr-smartdata-proyectofinal-dev-01`, que encadena los 5 notebooks en orden (ingestas → transformación Silver → agregaciones Golden) respetando sus dependencias.

![Workflow de producción](evidencias/Databricks/Workflow.png)

## Tablas Golden

### `ventas_diarias_tienda`
Agregación diaria por tienda (`03Transform_Gold_Ventas_Diarias_Tienda.ipynb`):

| Columna | Descripción |
|---------|-------------|
| `TIENDA`, `FECHA` | Llaves de agrupación |
| `NUM_PEDIDOS` | Pedidos distintos del día |
| `TOTAL_UNIDADES` | Unidades vendidas |
| `INGRESO_BRUTO` / `INGRESO_NETO` | Ingreso antes y después de aplicar el descuento |
| `DESCUENTO_MEDIO_PCT` | Descuento promedio (sólo líneas con descuento) |
| `NUM_LINEAS_CON_DESCUENTO` / `NUM_LINEAS_TOTAL` | Conteo de líneas con y sin descuento |
| `PCT_LINEAS_CON_DESCUENTO` | Porcentaje de líneas con descuento |
| `FECHA_ACTUALIZACION` | Marca de tiempo de la carga |

### `ventas_categoria_mes`
Agregación mensual por categoría de producto (`03Transform_Gold_Ventas_Categoria_Mes.ipynb`):

| Columna | Descripción |
|---------|-------------|
| `CATEGORIA`, `ANIO`, `MES` | Llaves de agrupación |
| `NUM_PEDIDOS` / `NUM_ARTICULOS` / `TOTAL_UNIDADES` | Métricas de volumen |
| `INGRESO_BRUTO` / `INGRESO_NETO` / `PRECIO_PROMEDIO` | Métricas de ingresos |
| `NUM_LINEAS_CON_DESCUENTO` / `PCT_LINEAS_CON_DESCUENTO` / `DESCUENTO_MEDIO_PCT` | Métricas de descuento |
| `NUM_PRECIO_NORMAL` / `NUM_PROMOCION` / ... | Conteo de líneas por etiqueta de descuento |

Ambas tablas se escriben con `coalesce(4)` y `mode("overwrite")` mediante `insertInto`.

---

## Dashboard

Sobre las tablas Golden se construyó un dashboard en Power BI (`dashboard/Dashboard.pbix`) con la carga de datos desde Databricks y dos vistas principales: ventas por mes y ventas por categoría y mes.

---

## Autor

Proyecto desarrollado por **Frank Torres** como entregable del curso de Ingeniería de Datos e IA en SmartData.
