# Borrador: secciones de Juan Pablo (§2 Marco teórico y §4.2 Versión PySpark)

> Texto para copiar en el informe:
> - **§2 Marco teórico:** va entre la §1 (Introducción) y la §3 (Entorno).
> - **§4.2 Versión con PySpark:** va después de la §4.1 de pandas. Sigue su mismo formato: lista de
>   módulos y luego cada etapa con una explicación breve y un fragmento de código.
>
> Al copiar a Word, convierta el formato Markdown:
> - `*texto*` pasa a cursiva y `` `texto` `` a fuente monoespaciada;
> - las tablas se numeran como `[Tabla N]`, para ajustarlas a la numeración del documento final;
> - las notas con ✎ son indicaciones para el equipo y no se copian.

---

## 2. Marco teórico

### 2.1 Big Data

Se denomina Big Data a los conjuntos de datos cuyo volumen, velocidad de generación o variedad
superan la capacidad de las herramientas convencionales de un solo equipo para almacenarlos y
procesarlos en un tiempo razonable (Laney, 2001). A esas tres dimensiones se suele añadir la
**veracidad**, es decir, la calidad y confiabilidad de los datos. El límite no es un tamaño fijo:
depende de la herramienta y del equipo. En este proyecto el límite práctico es la memoria RAM del
portátil de pruebas (7,64 GB), que el conjunto de datos supera en 1,5 veces.

Para procesar este tipo de datos, los sistemas modernos aplican dos ideas: **dividir los datos en
partes** que se procesan de forma independiente y en paralelo, y **llevar el cómputo hacia los
datos** en lugar de concentrar todos los datos en un único proceso. Apache Spark se basa en
ambas.

### 2.2 Limitaciones de pandas

pandas es la librería de referencia para el análisis de datos tabulares en Python (McKinney, 2022),
pero su diseño parte de tres supuestos que no se cumplen en un problema de Big Data:

- **Todo el DataFrame debe estar en memoria.** pandas no trabaja con datos en disco: para operar
  sobre una tabla tiene que cargarla completa en la RAM. Además, la representación en memoria es
  mayor que el archivo original. El propio autor de pandas recomienda disponer de 5 a 10 veces más
  RAM que el tamaño del conjunto de datos (McKinney, 2017), y en este proyecto se midió un factor de
  3,63 (Tabla 3).
- **Un solo nodo y, en la práctica, un solo núcleo.** Las operaciones se ejecutan en un único
  proceso de Python y la mayoría usan un solo hilo, de modo que no aprovechan los demás núcleos del
  procesador.
- **Ejecución inmediata.** Cada instrucción se ejecuta en el momento y su resultado intermedio
  ocupa memoria. pandas no conoce el flujo completo, por lo que no puede reordenarlo ni descartar
  columnas o filas que no se usarán más adelante.

La lectura por fragmentos (*chunks*) reduce el consumo de memoria, pero cada fragmento se procesa
de forma aislada. Por eso solo resuelve operaciones que se pueden combinar sumando resultados
parciales, como las sumas por grupo, y no las que necesitan ver todos los datos a la vez, como la
eliminación de duplicados, el conteo de valores únicos o los rankings.

### 2.3 Arquitectura de Apache Spark

Apache Spark es un motor de procesamiento distribuido que reparte el trabajo entre varios procesos
y, cuando hace falta, entre varias máquinas (Zaharia et al., 2016). Una aplicación de Spark tiene
tres componentes:

- **Driver:** el proceso principal. Ejecuta el programa del usuario, construye el plan de ejecución
  y lo divide en tareas.
- **Executors:** los procesos que ejecutan las tareas sobre los datos y guardan resultados en
  memoria o en disco.
- **Gestor de recursos** (*cluster manager*): asigna CPU y memoria a los executors.

Spark corre sobre la Máquina Virtual de Java (JVM). PySpark es su interfaz para Python: el
programa se escribe en Python, pero el procesamiento lo realiza la JVM.

En **modo local** (`local[*]`), el que usa este proyecto, el driver y los executors comparten una
única JVM en el mismo equipo, y cada núcleo lógico del procesador actúa como un executor. El
código es el mismo que se usaría en un clúster; solo cambia el parámetro `master`.

### 2.4 Modelo de ejecución: particiones, evaluación perezosa y DAG

**Particiones.** Spark divide cada conjunto de datos en particiones, que son fragmentos de hasta
128 MB al leer archivos. Cada partición se procesa en una **tarea** independiente, y se ejecutan
tantas tareas a la vez como núcleos haya. Ninguna partición necesita estar en memoria al mismo
tiempo que las demás, y si una no cabe, Spark escribe datos intermedios en disco (*spill*) en lugar
de fallar.

**Transformaciones y acciones.** Las operaciones de Spark son de dos tipos:

- las **transformaciones** (`read`, `filter`, `join`, `groupBy`, `withColumn`) no se ejecutan, solo
  se registran en un plan;
- las **acciones** (`count`, `show`, `write`) obligan a ejecutar ese plan.

Esta **evaluación perezosa** permite que Spark conozca el flujo completo antes de ejecutarlo y lo
optimice.

**DAG, etapas y *shuffle*.** El plan se representa como un grafo dirigido acíclico (**DAG**) de
operaciones (Zaharia et al., 2012). Spark lo divide en **etapas** (*stages*) según el tipo de
dependencia entre ellas:

- en las **transformaciones estrechas** (filtrar, calcular columnas), cada partición de salida
  depende de una sola partición de entrada y se procesa sin mover datos;
- en las **transformaciones amplias** (`groupBy`, `join`, eliminar duplicados), las filas con la
  misma clave deben reunirse. Spark las redistribuye entre particiones según el *hash* de la clave,
  en un proceso llamado ***shuffle***, que marca el límite entre una etapa y la siguiente.

El *shuffle* es lo que permite a Spark resolver con exactitud las operaciones que los *chunks* de
pandas no pueden: después de redistribuir los datos, todas las filas con el mismo identificador,
cliente o departamento están en la misma partición, sin importar en qué archivo estaban. Es también
la operación más costosa, porque escribe y lee datos intermedios en disco, de modo que un buen
diseño busca reducirla.

[✎ Figura sugerida: captura del DAG de la Spark UI (http://localhost:4040, pestaña SQL) durante la
ejecución de `main.py`, en la que se vean las etapas separadas por los *shuffles* (`Exchange`).]

### 2.5 DataFrames, Spark SQL y optimización

Los **DataFrames** de Spark son tablas distribuidas con columnas tipadas, semejantes a las de
pandas pero repartidas en particiones. Las consultas pueden escribirse con la API de DataFrames o
en **Spark SQL**. Ambas formas se traducen al mismo plan, que optimiza **Catalyst**, el optimizador
de consultas de Spark (Armbrust et al., 2015). Catalyst, entre otras cosas:

- aplica los filtros lo antes posible;
- descarta las columnas que no se usan;
- elige el tipo de join según el tamaño de las tablas.

Desde Spark 3, la **ejecución adaptativa** (AQE) reoptimiza el plan durante la ejecución con
estadísticas reales: por ejemplo, une particiones pequeñas después de un *shuffle*.

El proyecto aplica cuatro técnicas de optimización ([Tabla N]), que se describen en detalle en la
§4.2.

**[Tabla N]. Técnicas de optimización de Spark aplicadas en el proyecto**

| Técnica | En qué consiste | Uso en el proyecto |
|---|---|---|
| *Broadcast join* | Se envía una copia completa de la tabla pequeña a cada tarea, así la tabla grande se une sin *shuffle* | Joins con productos (60.000 filas) y tiendas (600) |
| *Caching* | Se guarda en memoria (y en disco si no cabe) un resultado que se reutiliza, para no recalcularlo | La base limpia y unida, que alimenta las cuatro consultas |
| Formato columnar Parquet | Almacenamiento comprimido por columnas: solo se leen las columnas necesarias | Escritura de los resultados y de la base limpia |
| Particionamiento | Los datos se organizan en carpetas por clave; un filtro por esa clave lee solo las carpetas necesarias (*partition pruning*) | Base limpia particionada por año y mes |

### Referencias para agregar (APA 7)

[✎ Agregar a la lista de referencias del informe, en orden alfabético con las existentes.]

Armbrust, M., Xin, R. S., Lian, C., Huai, Y., Liu, D., Bradley, J. K., Meng, X., Kaftan, T.,
Franklin, M. J., Ghodsi, A., & Zaharia, M. (2015). Spark SQL: Relational data processing in Spark.
En *Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data*
(pp. 1383–1394). ACM. https://doi.org/10.1145/2723372.2742797

Chambers, B., & Zaharia, M. (2018). *Spark: The definitive guide. Big data processing made simple*.
O'Reilly Media.

Laney, D. (2001). *3D data management: Controlling data volume, velocity and variety*. META Group.

McKinney, W. (2017, 21 de septiembre). *Apache Arrow and the "10 things I hate about pandas"*.
https://wesmckinney.com/blog/apache-arrow-pandas-internals/

Zaharia, M., Chowdhury, M., Das, T., Dave, A., Ma, J., McCauley, M., Franklin, M. J., Shenker, S.,
& Stoica, I. (2012). Resilient distributed datasets: A fault-tolerant abstraction for in-memory
cluster computing. En *9th USENIX Symposium on Networked Systems Design and Implementation (NSDI
12)* (pp. 15–28). USENIX Association.

Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A., Meng, X., Rosen, J.,
Venkataraman, S., Franklin, M. J., Ghodsi, A., Gonzalez, J., Shenker, S., & Stoica, I. (2016).
Apache Spark: A unified engine for big data processing. *Communications of the ACM, 59*(11), 56–65.
https://doi.org/10.1145/2934664

---

## 4.2 Versión con PySpark

La versión con PySpark se estructuró como un paquete modular dentro de la carpeta `version_spark/`,
con un módulo por etapa del pipeline, igual que la versión pandas, para facilitar la comparación.
No se limita a traducir el código de pandas: aprovecha el procesamiento en paralelo por
particiones, la evaluación perezosa y las optimizaciones propias de Spark.

- **sesion.py:** Creación de la SparkSession en modo local, con memoria y particiones calculadas según el equipo.
- **cargador.py:** Lectura de los CSV con esquema explícito, sin inferencia de tipos.
- **limpieza.py:** Deduplicación global, fechas en dos formatos, filtrado de anomalías y homologación de métodos de pago.
- **joins.py:** Broadcast join con productos y tiendas; sort-merge join con clientes.
- **agregaciones.py:** Las cuatro consultas de negocio con DataFrames, Spark SQL y funciones de ventana.
- **escritura.py:** Resultados en Parquet y base limpia en Parquet particionado por año y mes.
- **main.py:** Punto de entrada; mide el tiempo de cada etapa.
- **validar_equivalencia.py:** Comprueba que los resultados coinciden con los de pandas.

La versión Spark no necesita un módulo de mitigaciones, porque las tres técnicas de pandas ya forman
parte de su funcionamiento: los *chunks* corresponden a las particiones, `usecols` a la poda
automática de columnas y los `dtypes` al esquema de lectura.

### 4.2.1 Configuración de la sesión (sesion.py)

En modo `local[*]`, el driver y los executors comparten una sola JVM y cada núcleo procesa una
partición en paralelo. Los parámetros se calculan a partir del hardware, de modo que en el equipo de
pruebas (7,64 GB de RAM y 12 hilos) quedan en 4 GB de memoria y 36 particiones de *shuffle*:

```python
spark = (SparkSession.builder.appName("ventas-retail-spark")
         .master("local[*]")                                   # todos los núcleos
         .config("spark.driver.memory", memoria_driver)        # ~50 % de la RAM (4g)
         .config("spark.sql.shuffle.partitions", particiones)  # 3 x núcleos (36), no 200
         .config("spark.sql.adaptive.enabled", "true")         # AQE: reoptimiza en ejecución
         .getOrCreate())
```

### 4.2.2 Carga de datos con evaluación perezosa (cargador.py)

La lectura no carga datos en memoria: `spark.read.csv` solo construye un plan, que se ejecuta cuando
una acción (`count`, `write`) lo requiere. Así el optimizador Catalyst puede revisar el plan completo
antes de ejecutarlo. El esquema explícito evita `inferSchema`, que obligaría a recorrer los 11,5 GB
una vez más solo para deducir los tipos:

```python
def cargar_transacciones(spark, porcentaje):
    rutas = [str(p) for p in rutas_muestra(porcentaje)]   # mismos archivos que pandas
    return (spark.read.options(header="true")
                 .schema(esquema_spark_transacciones())    # tipos declarados (equivale a dtypes)
                 .csv(rutas))                              # perezoso: aún no se lee nada
```

### 4.2.3 Limpieza y deduplicación global (limpieza.py)

Esta es la diferencia más importante frente a la versión por *chunks*. `dropDuplicates`
redistribuye las filas por el hash de `transaccion_id` (*shuffle*), de modo que todas las copias de
un mismo id llegan a la misma partición, sin importar en qué archivo estaban. Las fechas se
interpretan según su formato, sin ambigüedad entre día y mes, y la limpieza usa funciones nativas en
lugar de funciones de Python fila a fila (UDF):

```python
def limpiar_transacciones(df):
    fecha = (F.when(F.col("fecha_hora").contains("/"),
                    F.to_timestamp("fecha_hora", "dd/MM/yyyy HH:mm"))   # formato alterno
              .otherwise(F.to_timestamp("fecha_hora", "yyyy-MM-dd HH:mm:ss")))
    return (df.dropDuplicates(["transaccion_id"])                       # GLOBAL, vía shuffle
              .where((F.col("cantidad") > 0) & (F.col("precio_unitario") > 0))
              .withColumn("metodo_pago", normalizar_metodo_pago(F.col("metodo_pago")))
              .withColumn("fecha_hora", fecha)
              .where(F.col("fecha_hora").isNotNull())
              .withColumn("anio_mes", F.date_format("fecha_hora", "yyyy-MM")))
```

### 4.2.4 Estrategias de join (joins.py)

El tipo de join se elige según el tamaño de cada dimensión. Productos (60.000 filas) y tiendas (600)
se difunden completas a cada tarea con `broadcast`, de modo que la tabla de hechos no se mueve.
Clientes (5 millones de filas) es demasiado grande para difundirse con 4 GB de memoria y se une con
un *sort-merge join*, solo en la consulta que la necesita y solo con las columnas indispensables:

```python
def unir_modelo_estrella(tx_limpio, dims):
    tx = tx_limpio.join(F.broadcast(dims["productos"].select(*COLUMNAS_PRODUCTO)),
                        on="producto_id", how="inner")           # BroadcastHashJoin
    return tx.join(F.broadcast(dims["tiendas"].select(*COLUMNAS_TIENDA)),
                   on="tienda_id", how="inner")                  # BroadcastHashJoin

# En la consulta 4: LEFT JOIN con clientes (SortMergeJoin), conserva las compras anónimas
compras.join(clientes.select("cliente_id", "segmento"), on="cliente_id", how="left")
```

### 4.2.5 Caché y consultas analíticas (agregaciones.py)

La base limpia y enriquecida se guarda en caché (`MEMORY_AND_DISK`) con solo las 13 columnas
necesarias, de modo que las cuatro consultas no repiten la lectura, la limpieza ni los joins. Si no
cabe en memoria, Spark guarda en disco la parte que sobra en lugar de fallar. Las consultas combinan
la API de DataFrames, Spark SQL y funciones de ventana. Destacan la consulta 3, que obtiene el top 10
con `rank()` por departamento, y la consulta 4, que calcula clientes únicos con `countDistinct`, la
métrica que la versión por *chunks* no puede resolver con exactitud:

```python
base = preparar_base(unir_modelo_estrella(limpiar_transacciones(tx), dims))
base = base.persist(StorageLevel.MEMORY_AND_DISK)      # caché reutilizada por las 4 consultas

# Consulta 3: top 10 por departamento con función de ventana
ventana = Window.partitionBy("departamento").orderBy(F.col("unidades_vendidas").desc())
top10 = por_producto.withColumn("ranking", F.rank().over(ventana)).where("ranking <= 10")

# Consulta 4: clientes únicos exactos por segmento y mes
(con_segmento.groupBy("segmento", "anio_mes")
             .agg(F.round(F.avg("venta_neta"), 2).alias("ticket_promedio"),
                  F.countDistinct("cliente_id").alias("clientes_unicos")))
```

La consulta 2 se escribió en Spark SQL. Con `CASE WHEN` resuelve en una sola pasada lo que en pandas
requiere tres `groupby` y dos `merge`.

### 4.2.6 Escritura en Parquet particionado (escritura.py)

Los resultados se escriben en Parquet con compresión Snappy en `resultados/spark/`. De forma
opcional, la base limpia se guarda particionada por año y mes. Así, una consulta posterior que filtre
por fecha solo lee las carpetas que necesita (*partition pruning*), y por ser un formato columnar
solo lee las columnas que usa:

```python
(base.repartition("anio", "mes")        # pocos archivos grandes por carpeta
     .write.mode("overwrite")
     .partitionBy("anio", "mes")        # resultados/spark/ventas_limpias/anio=2025/mes=12/
     .parquet(ruta))
```

### 4.2.7 Validación de equivalencia con pandas (validar_equivalencia.py)

Para comprobar que ambas versiones producen los mismos resultados, el script une las salidas de
pandas y Spark por sus columnas clave y compara cada métrica. Con la muestra del 1 %, las dos
versiones conservan las mismas 1.065.473 filas después de la limpieza, y las consultas 2 y 3
coinciden exactamente. Las consultas 1 y 4, que agrupan por mes, presentaban diferencias que se
deben a la lectura de fechas en pandas: `to_datetime(format="mixed")` interpreta `07/09/2023` como
9 de julio en lugar de 7 de septiembre. Al leer ambos formatos de forma explícita, las cuatro
consultas resultaron equivalentes.

[✎ Actualizar con el resultado de `validar_equivalencia.py` en el equipo de pruebas Windows.]

Además, `verificar_resultados.py` comprueba que los resultados son coherentes entre sí. Por ejemplo, la suma de
transacciones de la consulta 2 es igual al total de filas de la base, y un filtro por mes sobre el
Parquet particionado solo lee la carpeta de ese mes. Las diez comprobaciones fueron correctas.

### 4.2.8 Ejecución con el 100 % de los datos

[✎ Completar con los datos de `evidencias/spark_resumen_100.json` del equipo de pruebas.]

Con el conjunto completo (123,58 millones de filas, 11,53 GB en CSV), la versión Spark terminó el
procesamiento en [T] minutos y escribió los resultados de las cuatro consultas. Durante la ejecución
usó los 12 procesadores lógicos del equipo (Figura [N]), mientras que pandas usaba uno solo. El
consumo de memoria se mantuvo en unos [X] GB (Figura [N+1]), porque está limitado por
`spark.driver.memory`. Lo que no cabe en la caché o en el *shuffle* se escribe en el disco local en lugar
de agotar la RAM.

[✎ Figura: `evidencias/capturas_spark/03_terminal_100.png`. *Ejecución de la versión PySpark con
el 100 % de los datos.*]

[✎ Figura: `01_administrador_cpu.png`. *Uso de los 12 procesadores lógicos durante la lectura y
limpieza.*]

[✎ Figura: `02_administrador_procesos.png`. *Memoria del proceso de Spark (OpenJDK Platform
binary) durante la ejecución.*]
