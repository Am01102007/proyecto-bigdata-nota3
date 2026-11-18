# Pruebas de la versión PySpark en el equipo de Windows

Para: Samuel (portátil de pruebas: i5-13420H, 7,64 GB de RAM, Windows 11).
Tiempo estimado: unos 30 minutos, casi todo es esperar la ejecución del 100 %.

Objetivo:
1. Confirmar que el código Spark funciona en Windows.
2. Confirmar que da los mismos resultados que pandas.
3. Confirmar que procesa el 100 % de los datos.
4. Tomar las capturas para el informe.

Todos los comandos se ejecutan en PowerShell, desde la carpeta `proyecto_bigdata`.

---

## 0. Preparación

1. Cierra las demás aplicaciones (navegador, Teams, etc.) y deja el portátil conectado a la corriente.
2. Comprueba que el disco donde está `C:\bigdata\tmp` tenga **al menos 25 GB libres**. Spark escribe ahí
   los datos intermedios (*shuffle*) y la caché que no cabe en RAM.
3. Trae el código y activa el entorno:

```powershell
git fetch
git checkout version-spark
git pull
.\.venv\Scripts\Activate.ps1
$env:PYTHONUTF8 = "1"        # tildes correctas al guardar los logs
python scripts\verificar_entorno.py
```

**Resultado esperado:** `0 error(es)`. Si hay errores, la tabla 8 del informe (§3.7) tiene las soluciones
más comunes.

---

## 1. Corregir la lectura de fechas en pandas (`version_pandas/limpieza.py`)

Sin este cambio, las consultas 1 y 4 de pandas salen distintas a las de Spark. `pd.to_datetime(format="mixed")`
lee `07/09/2023` como 9 de julio en lugar de 7 de septiembre, y unas 0,2 % de las ventas caen en el mes
equivocado. En `normalizar_fechas`, reemplaza la línea `dt = pd.to_datetime(serie, format="mixed", errors="coerce")` por:

```python
    s = serie.astype("string")
    alt = s.str.contains("/", na=False)          # formato dd/mm/aaaa HH:MM
    dt = pd.to_datetime(s.where(~alt), format="%Y-%m-%d %H:%M:%S", errors="coerce")
    dt = dt.fillna(pd.to_datetime(s.where(alt), format="%d/%m/%Y %H:%M", errors="coerce"))
```

Lo probamos en la Mac: con este cambio, las 4 consultas coinciden.

---

## 2. Muestra del 1 % y equivalencia con pandas (unos 2 minutos)

```powershell
cmd /c "python version_spark\main.py --muestra 1 --perfil > evidencias\spark_1.txt 2>&1"
cmd /c "python version_pandas\main.py --muestra 1 > evidencias\pandas_1.txt 2>&1"
python version_spark\validar_equivalencia.py --etiqueta "muestra 1 % - Windows"
```

**Resultado esperado:**
- En `evidencias\spark_1.txt`:
  - `Archivos: 1 partes | Filas: 1,235,843`
  - `driver.memory=4g | sql.shuffle.partitions=36`
  - Unas **1,22 millones** de filas limpias. Anota la cifra exacta: va en la §4.2.7.
  - Filas por consulta: Q1 = 1.800, Q2 = 30, Q3 = 250 y Q4 = 180.
- La tabla del validador debe salir con las **4 consultas EQUIVALENTE**. Queda guardada en
  `evidencias\equivalencia_pandas_spark.md`.
  - Si Q1 y Q4 salen con DIFERENCIAS, no se aplicó el paso 1.

---

## 3. Ejecución con el 100 % (varios minutos)

Abre el **Administrador de tareas** antes de empezar. En Rendimiento > CPU, haz clic derecho en el gráfico y
elige "Cambiar gráfico a > Procesadores lógicos". Luego lanza:

```powershell
python version_spark\main.py --muestra 100 --guardar-base --explain --esperar
```

**Resultado esperado:**
- La primera línea dice `Archivos: 100 partes | Filas: 123,584,300 | Volumen CSV: 11.532 GB`.
- La etapa `[1/3]` es la más larga: lee los 11,5 GB, quita duplicados y hace los joins.
- Termina con `RESUMEN DE EJECUCIÓN EXITOSA`.
  - Filas limpias: unas **122 millones**.
  - Filas por consulta: las mismas que en el 1 % (1.800 / 30 / 250 / 180).
- La memoria total queda acotada: el proceso **OpenJDK Platform binary** (Java) más Python, entre
  4,5 y 5 GB. **No debe llegar al 93 % ni al colapso que se vio con pandas.**
  - En la Mac, con los mismos 4g, el pico fue de 4,58 GB.
  - La RAM de Spark está en el proceso **Java**, no en Python. Por eso Python aparece con muy poca memoria.
- Gracias a `--esperar`, al final el programa queda en pausa con la Spark UI abierta. Toma las capturas
  del paso 4 **antes** de pulsar Enter.
- Al cerrar puede salir un error de `ShutdownHookManager`. Es inofensivo (§3.7 del informe).

Después de pulsar Enter, guarda las métricas y comprueba la coherencia de los resultados:

```powershell
copy resultados\spark\resumen_ejecucion.json evidencias\spark_resumen_100.json
cmd /c "python version_spark\verificar_resultados.py > evidencias\spark_verificacion_100.txt 2>&1"
type evidencias\spark_verificacion_100.txt
```

**Resultado esperado:** todas las líneas `[OK]` y `Todas las comprobaciones correctas.`

---

## 4. Capturas para el informe

Guárdalas en `evidencias\capturas_spark\` con estos nombres:

| # | Archivo | Cuándo | Qué debe verse | Va en |
|---|---|---|---|---|
| 1 | `01_administrador_cpu.png` | Durante la etapa `[1/3]` del paso 3 | Rendimiento > CPU con los 12 procesadores lógicos ocupados (pandas usaba 1) | §4.2.8 |
| 2 | `02_administrador_procesos.png` | Durante la etapa `[1/3]` | Procesos ordenados por memoria: OpenJDK Platform binary y el % total de memoria | §4.2.8 |
| 3 | `03_terminal_100.png` | Al terminar, antes de pulsar Enter | La consola desde `Archivos: 100 partes` hasta `RESUMEN DE EJECUCIÓN EXITOSA` con el tiempo | §4.2.8 |
| 4 | `04_spark_ui_dag.png` | Antes de pulsar Enter | http://localhost:4040 > pestaña **SQL / DataFrame** > clic en la consulta que dice `count` (la de la caché). El DAG con los bloques **Exchange** (shuffle) y **BroadcastHashJoin** | §2.4 (figura sugerida) |
| 5 | `05_spark_ui_stages.png` | Antes de pulsar Enter | Pestaña **Stages**: lista de etapas con Duration, Input, Shuffle Read y Shuffle Write | §4.2.3 (opcional) |

---

## 5. Devolver los resultados

```powershell
git add evidencias version_pandas\limpieza.py
git commit -m "Pruebas PySpark en Windows: evidencias y corrección de fechas en pandas"
git push
```

Si prefieres no hacer commit, envía por el chat la carpeta `evidencias` completa. Lo que hace falta es:

- `spark_1.txt`, `pandas_1.txt` y `equivalencia_pandas_spark.md`;
- `spark_resumen_100.json` y `spark_verificacion_100.txt`;
- la carpeta `planes_spark\`;
- las capturas.

Si algo falla, envía el mensaje de error completo y la salida de `python scripts\verificar_entorno.py`.

> El benchmark de tiempo, RAM y CPU con 3 repeticiones lo hace Amed con su script. Estas pruebas solo
> validan que el código funciona y producen las evidencias de la versión Spark.
