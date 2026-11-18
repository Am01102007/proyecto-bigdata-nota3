# Versión PySpark - Proyecto Nota 3 (Big Data)

**Autor:** Juan Pablo Rodriguez Gamba  
**Módulo:** `version_spark`  
**Objetivo:** procesar el 100 % de las transacciones (más que la RAM del equipo) con el mismo
flujo que la versión pandas, aprovechando el modelo de ejecución de Spark: particiones en
paralelo, evaluación perezosa, *broadcast joins*, caché, Parquet particionado y *spill* a disco.

---

## 1. Estructura

```
version_spark/
├── LEEME.md                  <- este documento
├── PRUEBAS_WINDOWS.md        <- instrucciones de prueba en el equipo de Windows
├── __init__.py
├── sesion.py                 <- SparkSession local[*]: memoria, particiones, AQE
├── cargador.py               <- CSV con esquema explícito (sin inferSchema)
├── limpieza.py               <- dedupe GLOBAL, fechas en 2 formatos, filtros, método de pago
├── joins.py                  <- broadcast (productos, tiendas) y sort-merge (clientes)
├── agregaciones.py           <- 4 consultas: DataFrames, Spark SQL, ventana rank()
├── escritura.py              <- Parquet de resultados + base particionada por anio/mes
├── main.py                   <- CLI y función ejecutar_pipeline() para el benchmark
├── validar_equivalencia.py   <- compara los resultados con los de pandas
└── verificar_resultados.py   <- coherencia interna de los resultados y partition pruning
```

Cada módulo tiene su equivalente en `version_pandas/` para poder compararlos en el informe (§4).
`version_pandas/mitigaciones.py` no tiene par: en Spark no hacen falta *chunks*, porque el
particionado es la forma normal de trabajar.

---

## 2. Ejecución

Desde la raíz del proyecto (`proyecto_bigdata`), con el entorno virtual activado.

**Windows (PowerShell):**
```powershell
.\.venv\Scripts\Activate.ps1
python version_spark\main.py --muestra 1                  # prueba rápida
python version_spark\main.py --muestra 100                # conjunto completo
python version_spark\main.py --muestra 10 --explain --perfil
python version_spark\main.py --muestra 100 --guardar-base # + Parquet particionado
```

**macOS / Linux:** igual, con `source .venv/bin/activate` y `/` en las rutas.

| Opción | Efecto |
|---|---|
| `--muestra N` | 1, 10, 50 o 100 %: los primeros N % de archivos, los mismos que usa pandas |
| `--memoria-driver 4g` | `spark.driver.memory`. Por defecto, ~50 % de la RAM (con 7,64 GB queda en 4g) |
| `--particiones N` | `spark.sql.shuffle.partitions`. Por defecto, 3 × núcleos lógicos |
| `--sin-cache` | No guarda la base en caché (sirve para medir el efecto del *caching*) |
| `--guardar-base` | Escribe la base limpia en `resultados/spark/ventas_limpias/anio=…/mes=…/` |
| `--explain` | Guarda los planes físicos en `evidencias/planes_spark/` |
| `--perfil` | Cuenta los problemas de calidad del CSV crudo (hace una pasada extra) |
| `--esperar` | Al terminar, deja la Spark UI abierta hasta pulsar Enter (para las capturas) |

**Salidas:**
- `resultados/spark/<consulta>/part-*.parquet`
- `resultados/spark/resumen_ejecucion.json` (tiempos por etapa, filas y configuración).
- Mientras corre, la Spark UI está en http://localhost:4040 (sirve para las capturas de DAG y *stages*).
  Se cierra con `spark.stop()`; para revisarla al final, use `--esperar`.

---

## 3. Flujo y decisiones de diseño

```
CSV (N archivos) ──read.csv + esquema──► dropDuplicates(transaccion_id)  [shuffle]
   ──► filtros + fechas + método de pago ──► ⋈ broadcast(productos) ⋈ broadcast(tiendas)
   ──► métricas + proyección (13 columnas) ──► persist(MEMORY_AND_DISK) ──count()──►
        ├─ Q1 groupBy (DataFrames)            ─► Parquet
        ├─ Q2 Spark SQL (CASE WHEN)            ─► Parquet
        ├─ Q3 groupBy + rank() por depto       ─► Parquet
        ├─ Q4 ⋈ clientes (sort-merge, LEFT) + countDistinct ─► Parquet
        └─ (opcional) Parquet particionado por anio/mes
```

| Etapa | pandas | PySpark | Por qué |
|---|---|---|---|
| Lectura | `read_csv` carga todo en RAM (o por *chunks*) | `read.csv` con esquema: solo crea un plan, perezoso | Cada núcleo lee su trozo de archivo en paralelo; nada se carga hasta que hay una acción |
| Tipos | `dtype` compactos para ahorrar RAM | Esquema explícito (`comun/esquemas.py`) | Evita `inferSchema`, que obliga a leer todo el CSV una vez más |
| Duplicados | `drop_duplicates`, que por *chunks* solo es local | `dropDuplicates` **global** | El *shuffle* por hash de `transaccion_id` junta todas las copias de un id en la misma tarea |
| Fechas | `to_datetime(format="mixed")` | Patrón elegido según el texto (`/` → `dd/MM/yyyy HH:mm`) | Elimina la ambigüedad día/mes (ver §5) |
| Método de pago | `.map` con una función Python por fila | `lower`, `trim`, `translate`, `create_map` | Funciones nativas: el trabajo se queda en la JVM, sin UDF ni serialización a Python |
| Joins pequeños | `merge` | `broadcast()` explícito | Productos (60 k) y tiendas (600) se copian a cada tarea, y la tabla de hechos **no se mueve** |
| Join grande | `merge` con 5 M filas en RAM | Sort-merge join solo en Q4 y solo con 3 columnas | Clientes es demasiado grande para *broadcast* con 4 GB; solo Q4 necesita el segmento |
| Reutilización | Un DataFrame en memoria | `persist(MEMORY_AND_DISK)` de la base proyectada | Las 4 consultas leen la caché en vez de repetir lectura, limpieza y joins; lo que no cabe va a disco |
| Clientes únicos | `nunique` (por *chunks*: sets en RAM) | `countDistinct` (agregación en dos fases) | Exacto y con *spill* a disco si hace falta |
| Top 10 | `rank` por grupo | `Window.partitionBy("departamento")` + `rank()` | Mismo criterio de empates (`method="min"`) |
| Escritura | `to_parquet` | `write.parquet` y `partitionBy("anio","mes")` | Escritura paralela y atómica; poda de particiones y de columnas en lecturas posteriores |

**Configuración (`sesion.py`):** `local[*]`, AQE activado (fusiona particiones pequeñas y ajusta
joins en ejecución), `shuffle.partitions = 3 × núcleos` (el valor por defecto, 200, sobra en una
sola máquina) y zona horaria UTC (las fechas se toman tal cual, como en pandas). Todo se puede
cambiar con argumentos o con las variables `SPARK_DRIVER_MEMORY`, `SPARK_SHUFFLE_PARTITIONS` y
`SPARK_LOCAL_DIRS`.

**Evidencia en los planes** (`--explain`, carpeta `evidencias/planes_spark/`):
- `BroadcastHashJoin` en productos y tiendas.
- `SortMergeJoin` solo en la consulta 4.
- `Exchange hashpartitioning(transaccion_id)`: es la deduplicación global.
- `InMemoryTableScan`: las consultas leen de la caché.

---

## 4. Resultados de desarrollo (macOS, NO son las mediciones oficiales)

Medidos en el equipo de desarrollo (Apple Silicon, 12 núcleos, 16 GB, `driver.memory=8g`) con
un dataset de prueba de 1 GB (`generar_datos.py --tamano-gb 1 --partes 10`). Solo sirven para
comprobar que el código funciona. Las cifras del informe deben salir del portátil de pruebas con
Windows, igual que las de pandas.

| Prueba | Resultado |
|---|---|
| 1 archivo (1,07 M de filas) | Pipeline completo en 7,6 s |
| 10 archivos (10,7 M de filas, 1 GB) | 17,1 s con caché frente a 25,7 s sin caché |
| Base limpia en Parquet particionado | 1 GB de CSV pasa a 207 MB (36 carpetas `anio/mes`) |
| 10 archivos con `--memoria-driver 4g` (lo del equipo de pruebas) | 15,6 s; pico de RAM de Python + JVM: 4,58 GB |
| `verificar_resultados.py` sobre los 10 archivos | 10/10 comprobaciones correctas |
| Perfil de calidad (1 archivo) | 3.214 duplicados, 5.335 fechas `dd/mm`, 86.042 clientes nulos, 32.223 métodos de pago sucios |

---

## 5. Equivalencia con pandas

```
python version_pandas\main.py --muestra 1
python version_spark\main.py  --muestra 1
python version_spark\validar_equivalencia.py --etiqueta "muestra 1 %"
```

El script une los resultados de ambas versiones por sus claves y compara cada métrica:
- Conteos: deben ser idénticos.
- Montos: diferencia relativa ≤ 1e-4.
- Valores redondeados: ≤ 0,01 más la resolución de `float32`.

El informe queda en `evidencias/equivalencia_pandas_spark.md`. Resultado con la muestra del 1 %:

| Consulta | pandas tal como está | pandas con fechas corregidas |
|---|---|---|
| Q1 ventas y margen por mes | Diferencias | **Equivalente** |
| Q2 tasas por canal | **Equivalente** | **Equivalente** |
| Q3 top 10 por departamento | **Equivalente** | **Equivalente** |
| Q4 ticket y clientes por mes | Diferencias | **Equivalente** |

- **Q2 y Q3 coinciden exactamente.** Ninguna agrupa por mes, lo que confirma que la limpieza,
  la deduplicación y los joins son iguales en ambas versiones (también lo son las filas limpias:
  1.065.473 en las dos).
- **Diferencias en Q1 y Q4: error de fechas en pandas.** `pd.to_datetime(..., format="mixed")`
  interpreta `07/09/2023` (7 de septiembre) como **9 de julio**, porque supone mes/día. Afecta a
  las fechas `dd/mm` con día ≤ 12 (aprox. 0,2 % de las filas), que caen en el mes equivocado.
  Spark elige el patrón según el formato del texto, así que no tiene esa ambigüedad. Con las fechas
  de pandas leídas con los dos formatos explícitos, las 4 consultas son equivalentes. Corrección
  propuesta para `version_pandas/limpieza.py`:
  ```python
  s = serie.astype("string")
  alt = s.str.contains("/", na=False)
  dt = pd.to_datetime(s.where(~alt), format="%Y-%m-%d %H:%M:%S", errors="coerce")
  dt = dt.fillna(pd.to_datetime(s.where(alt), format="%d/%m/%Y %H:%M", errors="coerce"))
  ```
- **Diferencias de céntimos en el ticket promedio.** pandas guarda los montos en `float32`
  (mitigación de memoria), que cerca de 87.000 solo distingue saltos de unos 0,008. Por eso pandas
  guarda 87223.796875 donde Spark guarda 87223.81. No es un error de lógica.
- **El modo pandas por *chunks* (`--mitigado`) no es exacto,** porque la deduplicación es local a
  cada fragmento. Se puede comprobar con el mismo script y sirve como evidencia de la §5 del informe.

---

## 6. Notas para el benchmark (§6 del informe)

1. **La memoria de Spark está en la JVM, no en Python.** `python main.py` lanza un proceso
   `java` hijo, que es el que tiene el *heap* (hasta `spark.driver.memory`) y hace el trabajo. Si
   se mide solo el proceso Python (como `MonitorMemoria` de la versión pandas), Spark parecerá
   usar unos 100 MB. Hay que sumar el RSS del proceso y de **todos sus hijos**:
   ```python
   p = psutil.Process(pid)
   rss = p.memory_info().rss + sum(h.memory_info().rss for h in p.children(recursive=True))
   ```
   Lo más sencillo es lanzar cada repetición como subproceso
   (`python version_spark/main.py --muestra N`) y monitorear ese árbol de procesos desde fuera.
   Así cada repetición arranca una JVM limpia, igual que pandas arranca un intérprete limpio.
2. **El tiempo de arranque de la JVM (~1-3 s) se reporta aparte** (`tiempo_sesion_seg` en el
   JSON). Explica por qué pandas puede ganar con la muestra del 1 % (sobrecarga de Spark con datos
   pequeños), que es una de las conclusiones que pide el enunciado.
3. Desglose de tiempos por etapa: está en `resultados/spark/resumen_ejecucion.json` después de
   cada ejecución.
4. Para comparar con y sin caché: `--sin-cache`.
5. En el equipo de 7,64 GB la memoria por defecto queda en `4g`. Para que la comparación sea justa,
   conviene no cambiarla entre repeticiones.

---

## 7. Protocolo en el equipo de pruebas (Windows)

Los pasos están en [PRUEBAS_WINDOWS.md](PRUEBAS_WINDOWS.md):

1. Verificación del entorno.
2. Corrección de fechas en pandas.
3. Equivalencia con la muestra del 1 %.
4. Ejecución del 100 % con `--esperar`.
5. `verificar_resultados.py`.
6. Capturas para el informe.

Cada paso indica el resultado esperado.
