# Resumen del Proyecto - Pipeline de Ventas Retail con Arquitectura Medallion

Este proyecto consiste en un pipeline completo de ingeniería de datos construido sobre Azure Databricks, que procesa información de ventas y productos de una cadena retail siguiendo una arquitectura **Medallion (Bronze → Silver → Golden)**.

El objetivo es tomar los datos crudos de pedidos y artículos, limpiarlos, unirlos y transformarlos hasta llegar a tablas analíticas listas para el reporting (descuentos aplicados, ingresos por tienda y por categoría, etc.).

---

## Características principales

- Arquitectura Medallion completa (Bronze → Silver → Golden) sobre Unity Catalog
- Ingesta parametrizada con `dbutils.widgets` (catálogo, esquemas, nombre del storage) — sin valores fijos en el código
- Gobernanza de datos mediante External Locations y Storage Credentials de Unity Catalog
- Reglas de negocio aplicadas
- Agregaciones Golden listas para análisis: ventas diarias por tienda y ventas mensuales por categoría
- Pipeline orquestado como job de Databricks Workflows con dependencias entre tareas
- CI/CD con GitHub Actions que exporta, despliega y ejecuta automáticamente el pipeline en producción
- Notebook de reversión (`reversion/rollback.ipynb`) para limpiar tablas y datos del esquema completo
- Visualización de resultados en Power BI sobre las tablas Golden

---

## ¿De qué trata el proyecto?

El proyecto parte de dos archivos CSV crudos — `items.csv` (catálogo de productos) y `orders.csv` (pedidos de venta) — y construye un pipeline que los limpia, filtra, une y agrega hasta obtener métricas de negocio: ingresos brutos y netos, unidades vendidas, porcentaje de líneas con descuento, ticket promedio, etc., agrupadas por tienda/día y por categoría/mes.

Para esto fue necesario configurar un Data Lake en Azure (ADLS Gen2) con un contenedor por capa, gobernar el acceso mediante Unity Catalog (External Locations y Storage Credentials), parametrizar todos los notebooks con widgets (catálogo, esquemas, nombre del storage), y automatizar el despliegue a producción mediante GitHub Actions.


---


## 1) Estructura del repositorio

```
smartdata-ingenieriadatoseIA/
├── proceso/                                              # Notebooks del pipeline (orden de ejecución)
│   ├── 00.PrepararAmbiente                               # Setup: external locations, catálogo, esquemas y tablas vacías
│   ├── 01.IngestaProductos                               # [Bronze] raw → bronze.productos
│   ├── 01.IngestaVentas                                  # [Bronze] raw → bronze.ventas
│   ├── 02.Transform_Ventas_Productos                     # [Silver] bronze → silver.ventas_productos_categorias
│   ├── 03.Transform_Gold_Ventas_Diarias_Tienda           # [Gold]   silver → golden.ventas_diarias_tienda
│   ├── 03.Transform_Gold_Ventas_Categoria_Mes            # [Gold]   silver → golden.ventas_categoria_mes
│   └── 03.Transform_Gold_KPI_Vendedor_Mes                # [Gold]   silver → golden.kpi_vendedor_mes
├── seguridad/
│   └── 01.Grants                                         # Permisos (GRANT/REVOKE) sobre catálogo, esquemas y external locations
├── reversion/
│   └── Rollback                                          # Elimina tablas Delta y archivos ADLS de las 3 capas (DROP + rm)
├── datasets/                                             # CSVs de origen cargados al contenedor raw de ADLS
│   ├── items.csv                                         # Catálogo de productos (~39K registros)
│   └── orders.csv                                        # Pedidos de venta (~1M registros)
├── dashboard/
│   └── PowerBI_ProyectoFinal.pbix                        # Dashboard Power BI conectado a las tablas Golden de Databricks
├── evidencias/                                           # Capturas de pantalla de los recursos configurados
│   ├── Azure/                                            # Storage Account, contenedores ADLS y Key Vault
│   ├── Databricks/                                       # Unity Catalog, External Locations, Credential y Workflows
│   └── PowerBI/                                          # Dashboard conectado a Databricks
├── certificaciones/
│   ├── 3664_3_..._Databricks - Generic.pdf               # Certificación Databricks
│   └── Certificaciones_enlace.txt                        # Enlace a certificaciones adicionales
├── .github/workflows/                                    # CI/CD: despliegue y ejecución del pipeline en producción
├── .gitignore
└── readme.md
```

---

## 2) Dataset utilizado

Los datos provienen de dos archivos planos cargados al contenedor `raw`:

| Archivo | Descripción | Filas aprox. |
|---------|-------------|--------------|
| `items.csv` | Catálogo de productos — ID, nombre y categoría | 39.194 |
| `orders.csv` | Pedidos de venta — tienda, fecha, artículo, cantidad, precio, descuento, vendedor y estado | 1.090.380 |

## 3) Recursos utilizados

### Azure

| Recurso | Detalle |
|---|---|
| Storage Account (ADLS Gen2) | `adlsftrsmardatadev01` — Hierarchical Namespace habilitado |
| Contenedores ADLS | `raw`, `bronze`, `silver`, `golden` — uno por capa Medallion |
| Azure Key Vault | Gestión de secretos y credenciales del workspace |
| Azure Databricks Workspace | Workspace con Unity Catalog habilitado |

![Flujo de Trabajo](evidencias/Azure/Contenedores.png)
![Flujo de Trabajo](evidencias/Azure/resource_groups_detalle.png)

### Databricks

| Recurso | Nombre | Propósito |
|---|---|---|
| Cluster | `cl-ftr-smartdata-dev-01` | Cómputo para desarrollo y ejecución del pipeline |
| Storage Credential | `cred-ftr-smartdata-azure-dev-01` | Credencial unificada de acceso a ADLS Gen2 |
| External Locations | `extl-raw`, `extl-bronze`, `extl-silver`, `extl-golden`, `extl-metastore` | Una por contenedor ADLS + metastore |
| Unity Catalog — Catálogo | `cat_ftr_smartdata_dev` | Catálogo central del proyecto |
| Unity Catalog — Esquemas | `raw`, `bronze`, `silver`, `golden` | Un esquema por capa Medallion |
| Unity Catalog — Tablas Delta | 2 Bronze + 1 Silver + 3 Gold | Tablas persistidas en Delta Lake con campos de auditoría por capa |
| Job de orquestación | `job-ftr-smartdata-proyectofinal-dev-01` | 6 tareas en DAG: pipeline end-to-end Bronze → Silver → Gold |
| Delta Sharing | Share + Recipient | Publicación de las 3 tablas Gold a Power BI Desktop sin mover datos |
| Notebooks | `proceso/` (7), `seguridad/` (1), `reversion/` (1) | Lógica completa del pipeline por capa |
| CI/CD | GitHub Actions | Despliegue y ejecución automática del pipeline en producción |

![Flujo de Trabajo](evidencias/Databricks/Credential.png)
![Flujo de Trabajo](evidencias/Databricks/External_Locations.png)

## 4) Configuración inicial del ambiente

El notebook `00.PrepararAmbiente` se ejecuta **una sola vez** para provisionar el ambiente completo: External Locations, Storage Credential, catálogo `cat_ftr_smartdata_dev`, los 4 esquemas y todas las tablas Delta vacías con su DDL definitivo (incluyendo campos de auditoría por capa).

El notebook `01.Grants` centraliza toda la gestión de permisos mediante `GRANT`/`REVOKE` sobre catálogo, esquemas, tablas, job de orquestación y Delta Sharing, cubriendo el usuario `yinfaulk_test_24@hotmail.com` y el grupo `Developers`.

---


## 5) Arquitectura (Medallion)

Los datos se separan en contenedores distintos por capa dentro de ADLS Gen2 (`raw`, `bronze`, `silver`, `golden`), cada uno gobernado mediante una **External Location** (`extl-raw`, `extl-bronze`, `extl-silver`, `extl-golden`, `extl-metastore`) respaldada por una **Storage Credential**. Sobre esa base se organiza el catálogo con un esquema por capa:

| Esquema  | Contenedor ADLS | Contenido |
|----------|-----------------|-----------|
| `raw`    | `raw`           | Archivos CSV de origen (`orders.csv`, `items.csv`) |
| `bronze` | `bronze`        | Tablas Delta con los datos crudos ingeridos + columna `INGESTION_DATE` |
| `silver` | `silver`        | Tabla limpia y unida `ventas_productos_categorias` |
| `golden` | `golden`        | Tablas agregadas `ventas_diarias_tienda` y `ventas_categoria_mes` |




---

## 5.1 Capa Bronze

La capa Bronze representa el primer nivel de la arquitectura Medallion. En esta etapa los datos crudos del contenedor `raw` son ingeridos **sin transformaciones de negocio**, preservando la fidelidad del origen y enriqueciéndose únicamente con campos de auditoría para trazabilidad.

### Entradas (Inputs)

| Archivo fuente | Ubicación (ADLS Gen2) | Tabla Delta destino |
|---|---|---|
| `items.csv` | `abfss://raw@<storage>.dfs.core.windows.net/items.csv` | `cat_ftr_smartdata_dev.bronze.productos` |
| `orders.csv` | `abfss://raw@<storage>.dfs.core.windows.net/orders.csv` | `cat_ftr_smartdata_dev.bronze.ventas` |

### Proceso de ingesta

Los notebooks `01.IngestaProductos.ipynb` y `01.IngestaVentas.ipynb` ejecutan el siguiente flujo:

1. **Lectura parametrizada** — Los parámetros de conexión (`container`, `catalogo`, `esquema`, `storageName`) se obtienen desde `dbutils.widgets`, construyendo dinámicamente la ruta ABFSS del archivo fuente sin valores fijos en el código.
2. **Esquema explícito** — Se define un `StructType` antes de la lectura para garantizar el tipado correcto de cada columna y evitar inconsistencias por inferencia automática.
3. **Enriquecimiento con auditoría** — Se agregan columnas de trazabilidad mediante `withColumn` sin modificar los datos originales.
4. **Escritura en Delta** — El DataFrame se persiste en la tabla Delta de bronze con `mode("overwrite")` y `overwriteSchema=true`, permitiendo actualizaciones de esquema sin intervención manual.

### Campos de auditoría añadidos

| Campo | Tipo | Descripción | Función Spark |
|---|---|---|---|
| `INGESTION_DATE` | `timestamp` | Fecha y hora exacta en que el registro fue cargado en bronze | `current_timestamp()` |
| `SOURCE_FILE_NAME` | `string` | Ruta completa del archivo fuente en ADLS (trazabilidad de origen) | `col("_metadata.file_path")` |
| `INGESTION_USER` | `string` | Usuario o service principal que ejecutó la ingesta | `current_user()` |

> Estos tres campos permiten auditar **cuándo**, **desde dónde** y **quién** realizó cada carga, sin alterar los datos originales del negocio.

![Flujo de Trabajo](evidencias/Databricks/UC_Bronze.png)

---

## 5.2 Capa Silver

La capa Silver aplica reglas de negocio, limpieza y enriquecimiento sobre los datos crudos de Bronze. En esta etapa se realiza el join entre ambas fuentes, se filtran registros inválidos, se calculan métricas derivadas y se añaden dimensiones analíticas listas para ser consumidas por las capas Gold y Power BI.

### Entradas (Inputs)

| Tabla fuente | Capa | Descripción |
|---|---|---|
| `cat_ftr_smartdata_dev.bronze.ventas` | Bronze | Pedidos de venta — solo registros con `STATUS='VALID'` |
| `cat_ftr_smartdata_dev.bronze.productos` | Bronze | Catálogo de productos (∼39K artículos, usado como broadcast) |

### Proceso de transformación

El notebook `proceso/02.Transform_Ventas_Productos.ipynb` ejecuta el siguiente flujo:

1. **Proyección sin auditoría Bronze** — Se seleccionan solo los campos de negocio de ambas tablas; los campos `INGESTION_DATE`, `SOURCE_FILE_NAME` e `INGESTION_USER` de Bronze se descartan deliberadamente — Silver genera su propia trazabilidad.
2. **Filtrado de calidad** — Solo se procesan ventas con `STATUS='VALID'`. Se eliminan nulos en campos clave y registros fuera de rango lógico (CANTIDAD > 0, PRECIO > 0, DESCUENTO en [0, 100]).
3. **Normalización de texto** — PRODUCTO, CATEGORIA y TIENDA se estandarizan con `trim + uppercase` para garantizar consistencia en las agrupaciones Gold.
4. **Join broadcast** — El catálogo de productos (∼39K filas) se difunde con `F.broadcast()` para evitar un shuffle completo sobre el dataset de ventas (∼1M+ registros).
5. **Enriquecimiento analítico** — Se calculan etiqueta de descuento, importes financieros (bruto, descuento y neto), dimensiones temporales (año, mes, trimestre, día semana, fin de semana) y segmento de precio.
6. **Auditoría Silver** — Se añaden campos de trazabilidad propios de esta capa.
7. **Escritura en Delta** — Se persiste con `coalesce(4)`, `mode("overwrite")` y `overwriteSchema=true` en la tabla `silver.ventas_productos_categorias`.

### Tabla producida: `ventas_productos_categorias`

![Flujo de Trabajo](evidencias/Databricks/UC_Silver.png)

| Columna | Tipo | Descripción |
|---|---|---|
| `ID_ARTICULO` | `string` | Identificador del artículo |
| `PRODUCTO` | `string` | Nombre del producto (normalizado) |
| `CATEGORIA` | `string` | Categoría del producto (normalizada) |
| `ID_PEDIDO` | `string` | Identificador del pedido |
| `TIENDA` | `string` | Nombre de la tienda (normalizado) |
| `NUM_LINEA` | `string` | Número de línea dentro del pedido |
| `FECHA` | `date` | Fecha de la venta |
| `CANTIDAD` | `int` | Unidades vendidas |
| `PRECIO` | `double` | Precio unitario |
| `DESCUENTO` | `int` | Porcentaje de descuento aplicado |
| `ID_VENDEDOR` | `string` | Identificador del vendedor |
| `ETIQUETA_DESCUENTO` | `string` | Precio Normal / Promoción / Bono Empresarial / Regalo |
| `IMPORTE_BRUTO` | `double` | CANTIDAD × PRECIO |
| `IMPORTE_DESCUENTO` | `double` | Monto del descuento aplicado |
| `IMPORTE_NETO` | `double` | IMPORTE_BRUTO − IMPORTE_DESCUENTO |
| `ANIO` | `int` | Año de la venta |
| `MES` | `int` | Mes de la venta |
| `TRIMESTRE` | `int` | Trimestre de la venta (1–4) |
| `DIA_SEMANA` | `int` | Día de la semana (1=Dom, 7=Sáb) |
| `ES_FIN_SEMANA` | `boolean` | True si la venta ocurrió en sábado o domingo |
| `RANGO_PRECIO` | `string` | Económico / Estándar / Premium / Lujo |

### Campos de auditoría añadidos

| Campo | Tipo | Descripción | Función Spark |
|---|---|---|---|
| `FECHA_PROCESO` | `timestamp` | Fecha y hora de la transformación Silver | `current_timestamp()` |
| `INGESTION_USER` | `string` | Usuario o SP que ejecutó el proceso Silver | `current_user()` |

> Los campos de auditoría Bronze (`INGESTION_DATE`, `SOURCE_FILE_NAME`) **no se propagan** a Silver — cada capa mantiene su propia trazabilidad independiente.

---

## 5.3 Capa Gold

La capa Gold produce tablas **pre-agregadas y desnormalizadas**, optimizadas para consumo directo en Power BI sin necesidad de lógica adicional en los informes. Cada tabla define una perspectiva analítica distinta sobre los datos de ventas.

### Entradas (Inputs)

| Tabla fuente | Capa | Descripción |
|---|---|---|
| `cat_ftr_smartdata_dev.silver.ventas_productos_categorias` | Silver | Dataset enriquecido y validado con importes, dimensiones temporales y etiquetas ya calculados |

### Proceso de transformación

| Notebook | Tabla Gold producida | Granularidad |
|---|---|---|
| `03.Transform_Gold_Ventas_Diarias_Tienda.ipynb` | `ventas_diarias_tienda` | Tienda + Día |
| `03.Transform_Gold_Ventas_Categoria_Mes.ipynb` | `ventas_categoria_mes` | Categoría + Mes |
| `03.Transform_Gold_KPI_Vendedor_Mes.ipynb` | `kpi_vendedor_mes` | Vendedor + Tienda + Mes |

En todos los casos se consumen directamente `IMPORTE_BRUTO`, `IMPORTE_NETO`, `ANIO`, `MES` y `TRIMESTRE` desde Silver — sin recalcular desde `PRECIO` y `DESCUENTO`.

![Flujo de Trabajo](evidencias/Databricks/UC_Golden.png)

---

### Tabla `ventas_diarias_tienda`

KPIs diarios por tienda para análisis de tendencia y rendimiento de puntos de venta. `ANIO`, `MES` y `TRIMESTRE` se incluyen en la granularidad para habilitar filtros de tiempo en Power BI sin tabla de fechas adicional.

| Columna | Tipo | Descripción |
|---|---|---|
| `TIENDA` | `string` | Nombre de la tienda |
| `FECHA` | `date` | Fecha de la venta |
| `ANIO` / `MES` / `TRIMESTRE` | `int` | Dimensiones temporales para filtros en Power BI |
| `NUM_PEDIDOS` | `int` | Pedidos distintos del día |
| `TOTAL_UNIDADES` | `int` | Unidades vendidas |
| `NUM_LINEAS_TOTAL` | `int` | Líneas de pedido totales |
| `INGRESO_BRUTO` | `double` | Suma de IMPORTE_BRUTO |
| `INGRESO_NETO` | `double` | Suma de IMPORTE_NETO |
| `INGRESO_DESCUENTO` | `double` | Monto total de descuento aplicado |
| `DESCUENTO_MEDIO_PCT` | `double` | Descuento promedio en líneas con descuento |
| `NUM_LINEAS_CON_DESCUENTO` | `int` | Líneas con descuento > 0 |
| `PCT_LINEAS_CON_DESCUENTO` | `double` | Porcentaje de líneas con descuento |
| `TICKET_PROMEDIO` | `double` | Ingreso neto / número de pedidos |

---

### Tabla `ventas_categoria_mes`

KPIs mensuales por categoría de producto con mix completo de tipo de venta. Útil para comparativas de categorías, estacionalidad y efectividad de promociones en Power BI.

| Columna | Tipo | Descripción |
|---|---|---|
| `CATEGORIA` | `string` | Categoría de producto |
| `ANIO` / `MES` / `TRIMESTRE` | `int` | Dimensiones temporales |
| `NUM_PEDIDOS` / `NUM_ARTICULOS` / `TOTAL_UNIDADES` | `int` | Métricas de volumen |
| `INGRESO_BRUTO` / `INGRESO_NETO` | `double` | Métricas de ingresos |
| `TICKET_PROMEDIO` / `PRECIO_PROMEDIO` | `double` | Métricas de precio |
| `NUM_LINEAS_CON_DESCUENTO` / `PCT_LINEAS_CON_DESCUENTO` / `DESCUENTO_MEDIO_PCT` | `int` / `double` | Métricas de descuento |
| `NUM_PRECIO_NORMAL` / `NUM_PROMOCION` / `NUM_BONO_EMPRESARIAL` / `NUM_REGALO` | `int` | Conteo de líneas por etiqueta de descuento |

---

### Tabla `kpi_vendedor_mes`

KPIs mensuales por vendedor y tienda para análisis de rendimiento comercial individual. Habilita rankings de vendedores, comparativas entre tiendas y detección de outliers en Power BI.

| Columna | Tipo | Descripción |
|---|---|---|
| `ID_VENDEDOR` | `string` | Identificador del vendedor |
| `TIENDA` | `string` | Tienda a la que pertenece |
| `ANIO` / `MES` / `TRIMESTRE` | `int` | Dimensiones temporales |
| `NUM_PEDIDOS` / `NUM_ARTICULOS_DISTINTOS` / `TOTAL_UNIDADES` / `NUM_LINEAS` | `int` | Métricas de volumen |
| `INGRESO_BRUTO` / `INGRESO_NETO` / `INGRESO_DESCUENTO` | `double` | Métricas de ingresos |
| `DESCUENTO_MEDIO_PCT` / `PCT_LINEAS_CON_DESCUENTO` | `double` | Métricas de descuento |
| `TICKET_PROMEDIO` | `double` | Ingreso neto / número de pedidos |
| `NUM_PRECIO_NORMAL` / `NUM_PROMOCION` / `NUM_BONO_EMPRESARIAL` / `NUM_REGALO` | `int` | Mix de tipo de venta |

---

### Campos de auditoría Gold

| Campo | Tipo | Descripción | Función Spark |
|---|---|---|---|
| `FECHA_ACTUALIZACION` | `timestamp` | Cuándo se calculó la agregación Gold | `current_timestamp()` |
| `INGESTION_USER` | `string` | Usuario o SP que ejecutó el pipeline Gold | `current_user()` |

> Los campos de auditoría Silver (`FECHA_PROCESO`, `INGESTION_USER` a nivel registro) **no se propagan** a Gold — la capa Gold agrega filas y pierde la granularidad individual. El `INGESTION_USER` de Gold identifica quién lanzó el **proceso de agregación**, no quién ingirió el dato original.

---

## 6) Job de orquestación

El job **job-ftr-smartdata-proyectofinal-dev-01** coordina la ejecución completa del pipeline Medallion (Bronze → Silver → Gold) sobre el cluster `cl-ftr-smartdata-dev-01`. Está compuesto por **6 tareas** con dependencias explícitas: las dos ingestas Bronze arrancan en paralelo, la transformación Silver espera a ambas, y las tres agregaciones Gold se ejecutan en paralelo una vez que Silver completa.

#### Tareas y dependencias

| Tarea | Notebook | Capa | Tabla producida | Depende de |
|---|---|---|---|---|
| `task-01-ingest-bronze-items` | `01.IngestaProductos` | Bronze | `bronze.productos` | — |
| `task-02-ingest-bronze-ventas` | `01.IngestaVentas` | Bronze | `bronze.ventas` | — |
| `task-03-ingest-silver-ventasproductos` | `02.Transform_Ventas_Productos` | Silver | `silver.ventas_productos_categorias` | task-01 + task-02 |
| `task-04-ingest-gold-ventascategoria` | `03.Transform_Gold_Ventas_Categoria_Mes` | Gold | `golden.ventas_categoria_mes` | task-03 |
| `task-05-ingest-gold-ventasdiarias` | `03.Transform_Gold_Ventas_Diarias_Tienda` | Gold | `golden.ventas_diarias_tienda` | task-03 |
| `task-06-ingest-gold-kpivendedormes` | `03.Transform_Gold_KPI_Vendedor_Mes` | Gold | `golden.kpi_vendedor_mes` | task-03 |

#### Grafo de dependencias

```
[Bronze]  task-01 ─ IngestaProductos ─┐
                                       ├► [Silver] task-03 ─ Transform_Ventas_Productos ─┬► [Gold] task-04 ─ Ventas_Categoria_Mes
[Bronze]  task-02 ─ IngestaVentas ───┘                                              ├► [Gold] task-05 ─ Ventas_Diarias_Tienda
                                                                                     └► [Gold] task-06 ─ KPI_Vendedor_Mes
```

#### Configuración del job

| Parámetro | Valor |
|---|---|
| Cluster | `cl-ftr-smartdata-dev-01` |
| Runs concurrentes máximos | 1 |
| Cola habilitada | Sí |
| Política de ejecución de tareas | `ALL_SUCCESS` — si una tarea falla, las dependientes se cancelan |
| Optimización | `PERFORMANCE_OPTIMIZED` |

#### Job

![Flujo de Trabajo](evidencias/Databricks/Worflow.png)

---
## 7) Dashboard

El archivo `dashboard/PowerBI_ProyectoFinal.pbix` contiene el informe conectado en tiempo real a las tablas Gold de Databricks mediante **Delta Sharing**, sin necesidad de exportar ni mover datos fuera del Lakehouse.

### Conexión vía Delta Sharing

Delta Sharing es el protocolo abierto de Databricks para compartir tablas Delta Lake con clientes externos (como Power BI) de forma segura y sin duplicar datos. El flujo de conexión configurado es:

1. **Crear el Share en Unity Catalog** — se agrega un Share que expone las 3 tablas Gold del catálogo `cat_ftr_smartdata_dev.golden`.
2. **Crear el Recipient** — se genera un destinatario con su enlace de activación (`.share` credential file).
3. **Conectar Power BI Desktop** — mediante el conector nativo **Databricks (Delta Sharing)**, se proporciona el enlace de activación y se cargan las tablas directamente desde ADLS Gen2 sin intermediarios.

### Tablas Gold compartidas via Delta Sharing

| Tabla | Granularidad | Uso principal en Power BI |
|---|---|---|
| `golden.ventas_diarias_tienda` | Tienda + Día | Tendencia de ingresos, ranking de tiendas, evolución temporal |
| `golden.ventas_categoria_mes` | Categoría + Mes | Mix de categorías, estacionalidad, efectividad promocional |
| `golden.kpi_vendedor_mes` | Vendedor + Tienda + Mes | Ranking de vendedores, análisis de descuentos por comercial |

### Estructura del informe (2 hojas)

**Hoja 1 — Resumen Ejecutivo**
Respode: *¿cómo va el negocio en el período seleccionado?*
Slicers de cabecera: `ANIO`, `TRIMESTRE`, `MES`, `TIENDA`.

| Visual | Tipo | Descripción |
|---|---|---|
| KPI cards | Tarjetas | `INGRESO_NETO`, `TICKET_PROMEDIO`, `NUM_PEDIDOS`, `INGRESO_DESCUENTO` |
| Evolución de ingresos | Línea | `INGRESO_NETO` por `FECHA` |
| Comparativa de tiendas | Barras horizontales | `INGRESO_NETO` por `TIENDA` (ordenado desc.) |
| Mix de categorías | Donut | `INGRESO_NETO` por `CATEGORIA` |
| Impacto del descuento | Barras agrupadas | `INGRESO_BRUTO` vs `INGRESO_NETO` por `MES` |

![Dashboard Power BI](evidencias/PowerBI/PowerBI_01.png)

**Hoja 2 — Análisis Estratégico**
Responde: *¿dónde están las oportunidades y los riesgos?*
Slicers de cabecera: `ANIO`, `TRIMESTRE`, `CATEGORIA`, `TIENDA`.

| Visual | Tipo | Descripción |
|---|---|---|
| Ranking de vendedores | Tabla ordenada | `ID_VENDEDOR`, `INGRESO_NETO`, `TICKET_PROMEDIO`, `PCT_LINEAS_CON_DESCUENTO` |
| Mix tipo de venta | Barras apiladas 100% | `NUM_PRECIO_NORMAL` / `NUM_PROMOCION` / `NUM_BONO_EMPRESARIAL` / `NUM_REGALO` por `CATEGORIA` |
| Tendencia del descuento | Línea doble | `DESCUENTO_MEDIO_PCT` + `PCT_LINEAS_CON_DESCUENTO` por `MES` |
| Dispersión tienda/valor | Scatter | `PCT_LINEAS_CON_DESCUENTO` vs `TICKET_PROMEDIO` (tamaño burbuja: `NUM_PEDIDOS`) |

![Dashboard Power BI](evidencias/PowerBI/PowerBI_02.png)

---

## 8) Autor

Proyecto desarrollado por **Frank Torres** como entregable del curso de Ingeniería de Datos e IA en SmartData.
