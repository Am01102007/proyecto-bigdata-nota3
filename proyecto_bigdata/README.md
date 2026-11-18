# Proyecto Nota 3: Big Data con Python y PySpark

**Análisis de ventas retail omnicanal (Colombia 2023‑2025): pandas frente a Apache Spark**

El proyecto demuestra que consolidar y analizar cientos de millones de transacciones de venta, con
un volumen mayor que la RAM del equipo, no es viable con pandas y sí con PySpark. Ambas versiones
procesan los mismos datos, en el mismo equipo y con las mismas muestras (1 %, 10 %, 50 %, 100 %).

- Descripción del conjunto de datos: [`datos/DESCRIPCION_DATOS.md`](datos/DESCRIPCION_DATOS.md)
- Versiones probadas: Windows 10/11 64 bits · JDK 17 · Python 3.11 · Spark/PySpark 3.5.9 · Hadoop winutils 3.3.5

---

## Estructura

```
proyecto_bigdata/
├── README.md                     <- este archivo
├── requirements.txt              <- dependencias de Python (versiones fijas)
├── comun/
│   ├── muestras.py               <- selección de muestras 1/10/50/100 % (común a ambas versiones)
│   └── esquemas.py               <- tipos de datos para pandas y esquemas para Spark
├── scripts/
│   ├── configurar_entorno.ps1    <- instala Spark + winutils y configura variables (Windows)
│   ├── verificar_entorno.py      <- comprueba Java, Spark, winutils y hace una prueba real
│   ├── info_equipo.py            <- ficha de hardware/software del equipo de pruebas
│   └── generar_datos.py          <- genera el dataset sintético (por defecto 1,5 x RAM)
├── datos/
│   ├── DESCRIPCION_DATOS.md      <- origen, diccionario de datos, volumen, calidad
│   └── raw/                      <- (generado) CSV de transacciones y dimensiones; no se versiona
├── version_pandas/               <- implementación con pandas
├── version_spark/                <- implementación con PySpark
├── benchmark/                    <- medición de tiempo, RAM y CPU (3 repeticiones) y gráficos del §6
├── informe/                      <- generación de las secciones 6-8 y ensamblado del PDF final
├── resultados/                   <- (generado) salidas Parquet y métricas
└── evidencias/                   <- capturas, logs, info_equipo.md, verificacion_entorno.txt
```

---

## 1. Requisitos del equipo

| Recurso | Mínimo | Comentario |
|---|---|---|
| Sistema | Windows 10 (1803+) o 11, 64 bits | Se usa `tar`, que viene incluido en Windows |
| RAM | 8 GB | El generador ajusta el volumen a la RAM disponible |
| Disco libre | ≈ 1,6 × el tamaño de los datos | Con 16 GB de RAM: datos de 24 GB, así que se necesitan ~40 GB libres. Se recomienda SSD. |
| Software | JDK 17, Python 3.11 | Spark y winutils se instalan en los pasos siguientes |

> **Regla de oro en Windows:** ninguna ruta del entorno (JDK, Spark, Hadoop, proyecto) debe contener
> **espacios ni tildes**. Por eso se usan `C:\Java` y `C:\bigdata` en lugar de `C:\Program Files`.

---

## 2. Instalación

Hay dos caminos: el **automático** (pasos 1, 3 y luego el script) o el **manual** (pasos 1 a 6).

### Paso 1. Instalar Java JDK 17 (manual en ambos caminos)

1. Descargue **Eclipse Temurin JDK 17 (LTS)**, Windows x64, `.msi`, desde https://adoptium.net/temurin/releases/?version=17
2. Durante la instalación, en *Custom Setup*:
   - Cambie la carpeta de destino a **`C:\Java\jdk-17\`** (sin espacios).
   - Active **"Set JAVA_HOME variable"** y **"Add to PATH"**.
3. Abra una terminal **nueva** y compruebe:
   ```powershell
   java -version          # debe decir: openjdk version "17.0.x"
   echo $env:JAVA_HOME    # C:\Java\jdk-17\
   ```

> Spark 3.5 funciona con Java 8, 11 o 17. **No use Java 21 o superior**: Spark 3.5 no los soporta
> oficialmente, y Java 23+ produce el error `getSubject is supported only if a security manager is allowed`.

### Paso 2. Descargar Apache Spark 3.5.9 (manual; el script lo hace solo)

1. En https://spark.apache.org/downloads.html elija *Spark release* **3.5.9** y *package type*
   **Pre-built for Apache Hadoop 3.3 and later**, y descargue `spark-3.5.9-bin-hadoop3.tgz`.
2. Descomprímalo en `C:\bigdata`:
   ```powershell
   mkdir C:\bigdata
   tar -xzf $HOME\Downloads\spark-3.5.9-bin-hadoop3.tgz -C C:\bigdata
   ```
   Resultado: `C:\bigdata\spark-3.5.9-bin-hadoop3\bin\spark-submit.cmd`

### Paso 3. Instalar Python 3.11 (manual en ambos caminos)

1. Descargue **Python 3.11.x (64‑bit)** desde https://www.python.org/downloads/windows/
2. En el instalador marque **"Add python.exe to PATH"** y deje marcado el *py launcher*.
3. Desactive los alias de la Microsoft Store: *Configuración → Aplicaciones → Configuración avanzada
   de aplicaciones → Alias de ejecución de aplicaciones*, y apague `python.exe` y `python3.exe`.
4. Compruebe: `py -3.11 --version`

> **¿Por qué 3.11?** Es la versión recomendada para PySpark 3.5. Con 3.12 o 3.13 pueden aparecer
> errores en los *workers* de Python.

### Camino automático (después de los pasos 1 y 3)

Desde la raíz del proyecto, en PowerShell **normal** (no hace falta administrador):

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned       # solo la primera vez
.\scripts\configurar_entorno.ps1 -JavaHome "C:\Java\jdk-17"
```

El script descarga Spark y winutils, define `JAVA_HOME`, `SPARK_HOME`, `HADOOP_HOME`,
`PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON` y `SPARK_LOCAL_DIRS`, actualiza el `PATH`, crea `.venv`
con `requirements.txt` y ejecuta la verificación. Después **cierre y vuelva a abrir la terminal**.
Si todo sale `[OK]`, salte a la sección 3.

### Paso 4. winutils.exe y hadoop.dll (manual; el script lo hace solo)

Spark usa librerías de Hadoop para acceder al sistema de archivos. En Windows, Hadoop necesita dos
binarios nativos que no vienen con Spark:

1. Cree `C:\bigdata\hadoop\bin`.
2. Descargue de https://github.com/cdarlint/winutils, carpeta **`hadoop-3.3.5/bin`**, los archivos
   **`winutils.exe`** y **`hadoop.dll`**, y cópielos en `C:\bigdata\hadoop\bin`.
3. Si `winutils.exe` no se ejecuta, instale *Microsoft Visual C++ Redistributable* (x64).

### Paso 5. Variables de entorno (manual; el script lo hace solo)

*Inicio → "Editar las variables de entorno de esta cuenta"* → **Variables de usuario**:

| Variable | Valor |
|---|---|
| `JAVA_HOME` | `C:\Java\jdk-17` |
| `SPARK_HOME` | `C:\bigdata\spark-3.5.9-bin-hadoop3` |
| `HADOOP_HOME` | `C:\bigdata\hadoop` |
| `PYSPARK_PYTHON` | `<ruta del proyecto>\.venv\Scripts\python.exe` |
| `PYSPARK_DRIVER_PYTHON` | `<ruta del proyecto>\.venv\Scripts\python.exe` |
| `SPARK_LOCAL_DIRS` | `C:\bigdata\tmp` (carpeta para *shuffle* y *spill*; debe tener espacio) |
| `Path` (agregar) | `%JAVA_HOME%\bin`, `%SPARK_HOME%\bin`, `%HADOOP_HOME%\bin` |

Equivalente en PowerShell:

```powershell
[Environment]::SetEnvironmentVariable("JAVA_HOME",   "C:\Java\jdk-17", "User")
[Environment]::SetEnvironmentVariable("SPARK_HOME",  "C:\bigdata\spark-3.5.9-bin-hadoop3", "User")
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\bigdata\hadoop", "User")
```

### Paso 6. Entorno virtual de Python (manual; el script lo hace solo)

```powershell
cd <ruta del proyecto>
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

> La versión de `pyspark` (pip) debe coincidir **exactamente** con la de `SPARK_HOME` (3.5.9).

---

## 3. Verificación del entorno

```powershell
.\.venv\Scripts\Activate.ps1
python scripts\verificar_entorno.py > evidencias\verificacion_entorno.txt 2>&1 ; type evidencias\verificacion_entorno.txt
python scripts\info_equipo.py          # genera evidencias\info_equipo.md (procesador, RAM, disco, versiones)
```

`verificar_entorno.py` revisa Python, librerías, Java, `SPARK_HOME`, winutils/hadoop.dll y ejecuta un
trabajo real de Spark (agregación, UDF de Python, escritura y lectura de Parquet). Si algo falla,
imprime la causa probable y la solución.

---

## 4. Generar los datos

```powershell
python scripts\generar_datos.py                                    # 1,5 x RAM del equipo (entrega)
python scripts\generar_datos.py --tamano-gb 1 --partes 10 --forzar # prueba rápida (~30 s)
python comun\muestras.py                                           # tamaño de cada muestra
```

Los datos quedan en `datos\raw\` (no se suben al repositorio). Ver
[`datos/DESCRIPCION_DATOS.md`](datos/DESCRIPCION_DATOS.md) para el diccionario de datos y la justificación.

## 5. Ejecutar las versiones

```powershell
# Versión pandas
python version_pandas\main.py --muestra 10

# Versión PySpark
python version_spark\main.py --muestra 10
```

*(Los parámetros finales los documenta cada versión en su propia carpeta.)*

### Benchmark comparativo (§6 del informe)

```powershell
python benchmark\medir.py              # 3 versiones x 4 muestras x 3 repeticiones (2-4 h en 8 GB)
python benchmark\graficos.py           # tablas y figuras en evidencias\benchmark\
python benchmark\complejidad_codigo.py # métricas de complejidad del código
python informe\generar_secciones.py    # secciones 6, 7 y 8 con las cifras medidas
python informe\ensamblar_pdf.py        # PDF final (requiere pandoc y Chrome o Edge)
```

Detalles en [`benchmark/LEEME.md`](benchmark/LEEME.md).

### Configuración recomendada de Spark en modo local

En modo `local[*]` el *driver* y los *executors* comparten un único proceso JVM, así que la memoria
que importa es `spark.driver.memory`. Debe fijarse **antes** de crear la primera `SparkSession`:

```python
spark = (SparkSession.builder
         .master("local[*]")                              # todos los núcleos
         .config("spark.driver.memory", "8g")             # ~50-60 % de la RAM física
         .config("spark.sql.shuffle.partitions", "64")    # ~2-4 x núcleos; el default 200 sobra en local
         .config("spark.sql.adaptive.enabled", "true")    # AQE: ajusta particiones y joins en ejecución
         .config("spark.local.dir", r"C:\bigdata\tmp")    # spill y shuffle en disco con espacio
         .getOrCreate())
```

| RAM del equipo | `spark.driver.memory` sugerido |
|---|---|
| 8 GB | `4g` |
| 16 GB | `8g`‑`10g` |
| 32 GB | `18g`‑`20g` |

---

## 6. Solución de problemas en Windows

| Error | Causa | Solución |
|---|---|---|
| `Java gateway process exited before sending its port number` | `JAVA_HOME` vacío, mal escrito o con espacios | Revise `echo $env:JAVA_HOME`; use `C:\Java\jdk-17` |
| `Could not locate executable null\bin\winutils.exe` | Falta `HADOOP_HOME` | Paso 4 y 5 |
| `UnsatisfiedLinkError ... NativeIO$Windows.access0` al escribir Parquet | Falta `hadoop.dll` o no está en el `PATH` | Copie `hadoop.dll` en `%HADOOP_HOME%\bin`, agregue esa carpeta al `PATH` y reinicie la terminal |
| `Python worker failed to connect back` | Spark lanza un Python distinto al del venv | Defina `PYSPARK_PYTHON` y `PYSPARK_DRIVER_PYTHON` |
| `Python was not found; run without arguments to install from the Microsoft Store` | Alias de la Microsoft Store | Desactive los alias (paso 3) |
| `UnsupportedClassVersionError` / `getSubject is supported only…` | Java 21 o superior | Instale JDK 17 |
| `ERROR ShutdownHookManager: Exception while deleting Spark temp dir` al cerrar | Windows bloquea archivos abiertos | Inofensivo; se puede ignorar o borrar `C:\bigdata\tmp` a mano |
| `java.lang.OutOfMemoryError: Java heap space` | Poca memoria para el driver | Suba `spark.driver.memory` o aumente `spark.sql.shuffle.partitions` |
| `No space left on device` | Disco lleno por *shuffle* | Libere espacio o apunte `SPARK_LOCAL_DIRS` a otro disco |
| `.ps1 cannot be loaded because running scripts is disabled` | Política de ejecución | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |

---

## 7. Fuentes y créditos

- Apache Software Foundation. *Apache Spark 3.5.9* [Software]. https://spark.apache.org
- Eclipse Adoptium. *Temurin JDK 17* [Software]. https://adoptium.net
- cdarlint. *winutils: binarios de Hadoop para Windows* [Repositorio]. https://github.com/cdarlint/winutils
- The pandas development team. *pandas 2.2.3* [Software]. https://pandas.pydata.org
- Apache Arrow. *PyArrow 17.0.0* [Software]. https://arrow.apache.org
- Datos: sintéticos, generados con `scripts/generar_datos.py` (código propio del equipo).
