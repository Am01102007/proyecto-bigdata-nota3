"""
verificar_entorno.py
====================
Comprueba, paso a paso, que el entorno Windows está listo para el proyecto:
Python, librerías, Java, variables de entorno, winutils/hadoop.dll y un trabajo
real de Spark que escribe y lee Parquet (la prueba que más falla en Windows).

Uso:
    python scripts/verificar_entorno.py

Cada comprobación imprime [OK], [AVISO] o [ERROR] con la causa y la solución.
Termina con código 0 si no hay errores. Guarde la salida como evidencia:
    python scripts/verificar_entorno.py > evidencias/verificacion_entorno.txt 2>&1
"""

from __future__ import annotations

import importlib
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ES_WINDOWS = platform.system() == "Windows"
errores: list[str] = []
avisos: list[str] = []


def ok(msg: str) -> None:
    print(f"[OK]     {msg}")


def aviso(msg: str, solucion: str = "") -> None:
    avisos.append(msg)
    print(f"[AVISO]  {msg}" + (f"\n         -> {solucion}" if solucion else ""))


def error(msg: str, solucion: str = "") -> None:
    errores.append(msg)
    print(f"[ERROR]  {msg}" + (f"\n         -> {solucion}" if solucion else ""))


def titulo(t: str) -> None:
    print(f"\n=== {t} " + "=" * (60 - len(t)))


# ---------------------------------------------------------------------------
def revisar_python() -> None:
    titulo("Python")
    v = sys.version_info
    print(f"         {sys.executable}  ({platform.python_version()})")
    if v < (3, 9):
        error("Python < 3.9 (el enunciado exige 3.9+)", "Instale Python 3.11 de python.org")
    elif v >= (3, 12):
        aviso(f"Python {v.major}.{v.minor}: se recomienda 3.11 con PySpark 3.5",
              "Recomendado Python 3.11 para evitar incompatibilidades")
    else:
        ok(f"Python {v.major}.{v.minor}")
    if ES_WINDOWS and "WindowsApps" in sys.executable:
        error("Está usando el Python de Microsoft Store",
              "Instale Python de python.org y desactive los 'alias de ejecución de aplicaciones'")


def revisar_librerias() -> None:
    titulo("Librerías")
    for mod in ["pyspark", "pandas", "numpy", "pyarrow", "psutil", "matplotlib"]:
        try:
            m = importlib.import_module(mod)
            ok(f"{mod:<11} {getattr(m, '__version__', '?')}")
        except ImportError:
            error(f"Falta {mod}", "pip install -r requirements.txt")


def version_java(java: str) -> tuple[int | None, str]:
    try:
        r = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=30)
    except Exception as e:  # noqa: BLE001
        return None, str(e)
    salida = (r.stderr or r.stdout).strip()
    m = re.search(r'version "(\d+)(?:\.(\d+))?', salida)
    if not m:
        return None, salida
    mayor = int(m.group(1))
    if mayor == 1 and m.group(2):  # formato 1.8.0_xxx = Java 8
        mayor = int(m.group(2))
    return mayor, salida.splitlines()[0]


def revisar_java() -> None:
    titulo("Java (JDK)")
    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        error("JAVA_HOME no está definida", "Ver README, paso 2")
        return
    print(f"         JAVA_HOME = {java_home}")
    if " " in java_home:
        aviso("JAVA_HOME contiene espacios (p. ej. 'Program Files')",
              "Spark puede fallar con rutas con espacios. Instale el JDK en C:\\Java\\jdk-17 "
              "o use la ruta corta C:\\PROGRA~1\\...")
    exe = Path(java_home) / "bin" / ("java.exe" if ES_WINDOWS else "java")
    if not exe.exists():
        error(f"No existe {exe}", "JAVA_HOME debe apuntar a la carpeta del JDK (la que contiene bin\\)")
        return
    mayor, linea = version_java(str(exe))
    print(f"         {linea}")
    if mayor in (8, 11, 17):
        ok(f"Java {mayor} (soportado por Spark 3.5 y permitido por el enunciado)")
    else:
        error(f"Java {mayor} no recomendado", "Use JDK 17 (o 11 / 8)")
    en_path = shutil.which("java")
    if en_path and Path(en_path).resolve() != exe.resolve():
        mayor_path, _ = version_java(en_path)
        if mayor_path != mayor:
            aviso(f"El 'java' del PATH ({en_path}, Java {mayor_path}) no es el de JAVA_HOME",
                  "Ponga %JAVA_HOME%\\bin al inicio del PATH")


def revisar_spark_home() -> None:
    titulo("Spark")
    spark_home = os.environ.get("SPARK_HOME")
    try:
        import pyspark
        v_pip = pyspark.__version__
    except ImportError:
        v_pip = None
    if not spark_home:
        error("SPARK_HOME no está definida", "Ver README, paso 3")
        return
    print(f"         SPARK_HOME = {spark_home}")
    if " " in spark_home:
        aviso("SPARK_HOME contiene espacios", "Use C:\\spark\\spark-3.5.9-bin-hadoop3")
    jars = Path(spark_home) / "jars"
    if not jars.exists():
        error(f"No existe {jars}", "SPARK_HOME debe apuntar a la carpeta descomprimida de Spark")
        return
    core = list(jars.glob("spark-core_*.jar"))
    v_dist = re.search(r"spark-core_[\d.]+-([\d.]+)\.jar", core[0].name).group(1) if core else "?"
    ok(f"Distribución Spark {v_dist}")
    if v_pip and v_dist != "?" and v_pip != v_dist:
        error(f"pyspark de pip ({v_pip}) != Spark de SPARK_HOME ({v_dist})",
              f"pip install pyspark=={v_dist}  (deben coincidir exactamente)")
    py = os.environ.get("PYSPARK_PYTHON")
    if not py:
        aviso("PYSPARK_PYTHON no está definida",
              "Defínala con la ruta del python del entorno virtual (evita 'Python worker failed to connect back')")
    else:
        ok(f"PYSPARK_PYTHON = {py}")


def revisar_hadoop() -> None:
    titulo("Hadoop / winutils")
    if not ES_WINDOWS:
        ok("No es Windows: winutils no es necesario")
        return
    hh = os.environ.get("HADOOP_HOME")
    if not hh:
        error("HADOOP_HOME no está definida", "Ver README, paso 4")
        return
    print(f"         HADOOP_HOME = {hh}")
    for f in ["winutils.exe", "hadoop.dll"]:
        p = Path(hh) / "bin" / f
        if p.exists():
            ok(f"{p}")
        else:
            error(f"Falta {p}", "Descargue winutils.exe y hadoop.dll de la versión Hadoop 3.3.x")
    path = os.environ.get("PATH", "").lower()
    if str(Path(hh) / "bin").lower() not in path and "%hadoop_home%\\bin" not in path:
        aviso("%HADOOP_HOME%\\bin no está en el PATH", "Agréguelo (hadoop.dll debe poder cargarse)")
    # Prueba de winutils
    w = Path(hh) / "bin" / "winutils.exe"
    if w.exists():
        r = subprocess.run([str(w), "ls", str(Path(hh))], capture_output=True, text=True)
        if r.returncode == 0:
            ok("winutils.exe se ejecuta correctamente")
        else:
            error("winutils.exe no se ejecuta",
                  "Instale 'Microsoft Visual C++ Redistributable' (x64) y reintente")


def prueba_spark() -> None:
    titulo("Prueba real de Spark (DataFrame -> Parquet -> lectura)")
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        error("No se pudo importar pyspark")
        return
    tmp = Path(tempfile.mkdtemp(prefix="spark_test_"))
    try:
        spark = (SparkSession.builder.master("local[*]").appName("verificar_entorno")
                 .config("spark.ui.showConsoleProgress", "false")
                 .config("spark.sql.shuffle.partitions", "4")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        ok(f"SparkSession creada (Spark {spark.version}, {spark.sparkContext.defaultParallelism} núcleos)")
        df = spark.range(1_000_000).withColumn("grupo", F.col("id") % 10)
        agg = df.groupBy("grupo").agg(F.count("*").alias("n")).orderBy("grupo")
        n = agg.collect()
        assert len(n) == 10 and n[0]["n"] == 100_000
        ok("Agregación por grupos (requiere workers de Python: OK)")
        # UDF de Python: obliga a lanzar un proceso Python worker
        from pyspark.sql.types import IntegerType
        doble = F.udf(lambda x: x * 2, IntegerType())
        assert df.limit(5).select(doble(F.col("grupo").cast("int"))).count() == 5
        ok("UDF de Python (PYSPARK_PYTHON correcto)")
        ruta = str(tmp / "prueba.parquet")
        agg.write.mode("overwrite").parquet(ruta)
        assert spark.read.parquet(ruta).count() == 10
        ok("Escritura y lectura Parquet (winutils/hadoop.dll correctos)")
        spark.stop()
    except Exception as e:  # noqa: BLE001
        msg = str(e).splitlines()[0][:300]
        error(f"Falló la prueba de Spark: {msg}", diagnosticar(str(e)))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def diagnosticar(e: str) -> str:
    """Traduce los errores típicos de Spark en Windows a su solución."""
    reglas = [
        ("JAVA_GATEWAY_EXITED", "Java no arranca: revise JAVA_HOME (sin espacios) y la versión del JDK"),
        ("Java gateway process exited", "Java no arranca: revise JAVA_HOME (sin espacios) y la versión del JDK"),
        ("winutils", "Falta winutils.exe en %HADOOP_HOME%\\bin o HADOOP_HOME mal definida"),
        ("NativeIO$Windows.access0", "Falta hadoop.dll en %HADOOP_HOME%\\bin (y en el PATH)"),
        ("UnsatisfiedLinkError", "Falta hadoop.dll o no coincide con la versión de Hadoop (use 3.3.x)"),
        ("Python worker failed to connect back", "Defina PYSPARK_PYTHON con la ruta del python del venv"),
        ("Python was not found", "Defina PYSPARK_PYTHON y desactive los alias de Python de Microsoft Store"),
        ("UnsupportedClassVersionError", "Versión de Java incompatible: use JDK 17"),
        ("getSubject is supported only", "Java 23+ no es compatible: use JDK 17"),
        ("Address already in use", "Puerto ocupado: cierre otras sesiones de Spark o reinicie"),
    ]
    for clave, sol in reglas:
        if clave.lower() in e.lower():
            return sol
    return "Revise el mensaje completo y la sección 'Solución de problemas' del README"


def main() -> int:
    print(f"Sistema: {platform.system()} {platform.release()} ({platform.version()})")
    revisar_python()
    revisar_librerias()
    revisar_java()
    revisar_spark_home()
    revisar_hadoop()
    if not any("JAVA_HOME" in e or "pyspark" in e for e in errores):
        prueba_spark()
    titulo("Resumen")
    print(f"{len(errores)} error(es), {len(avisos)} aviso(s)")
    if not errores:
        print("El entorno está listo para ejecutar el proyecto.")
    return 1 if errores else 0


if __name__ == "__main__":
    sys.exit(main())
