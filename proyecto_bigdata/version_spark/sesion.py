"""
version_spark/sesion.py
=======================
Creación de la SparkSession para modo local (`local[*]`).

En modo local el driver y los executors comparten UNA sola JVM: cada núcleo lógico
ejecuta una tarea (una partición) en paralelo. Por eso la memoria útil para Spark es
`spark.driver.memory`, que debe fijarse ANTES de crear la primera sesión (después ya
no tiene efecto porque la JVM ya arrancó).

Los valores por defecto se calculan a partir del equipo, así el mismo código sirve en
el portátil de pruebas (8 GB) y en equipos más grandes sin tocar nada:

    spark.driver.memory          ~50 % de la RAM física (mín. 2g, máx. 24g)
    spark.sql.shuffle.partitions 3 x núcleos lógicos (el valor de fábrica, 200, crea
                                 demasiadas tareas pequeñas en una sola máquina)

Todos se pueden sobrescribir por argumento o con variables de entorno
(SPARK_DRIVER_MEMORY, SPARK_SHUFFLE_PARTITIONS, SPARK_LOCAL_DIRS).
"""

from __future__ import annotations

import os
from pathlib import Path

import psutil
from pyspark.sql import SparkSession

RAIZ = Path(__file__).resolve().parents[1]


def memoria_driver_sugerida() -> str:
    """~50 % de la RAM física, redondeado a GB (deja memoria al SO y a los workers de Python)."""
    ram_gb = psutil.virtual_memory().total / 1024**3
    return f"{round(min(24, max(2, ram_gb * 0.5)))}g"   # 7,64 GB -> 4g ; 16 GB -> 8g


def particiones_shuffle_sugeridas() -> int:
    """3 x núcleos lógicos: suficientes tareas para paralelizar sin sobrecarga de planificación."""
    return 3 * (os.cpu_count() or 4)


def crear_sesion(
    nombre_app: str = "ventas-retail-spark",
    memoria_driver: str | None = None,
    particiones_shuffle: int | None = None,
    dir_local: str | None = None,
    nivel_log: str = "WARN",
) -> SparkSession:
    """Crea (o reutiliza) la SparkSession con la configuración del proyecto.

    Args:
        nombre_app: Nombre visible en la Spark UI (http://localhost:4040).
        memoria_driver: Heap de la JVM, p. ej. "4g". Por defecto ~50 % de la RAM.
        particiones_shuffle: Particiones tras un shuffle (groupBy, join, distinct).
        dir_local: Carpeta para shuffle y spill a disco. Por defecto SPARK_LOCAL_DIRS
                   o la carpeta temporal del sistema.
        nivel_log: Nivel de log de Spark (WARN evita ruido en consola).
    """
    memoria_driver = memoria_driver or os.environ.get("SPARK_DRIVER_MEMORY") or memoria_driver_sugerida()
    particiones_shuffle = particiones_shuffle or int(
        os.environ.get("SPARK_SHUFFLE_PARTITIONS", particiones_shuffle_sugeridas())
    )
    dir_local = dir_local or os.environ.get("SPARK_LOCAL_DIRS")

    constructor = (
        SparkSession.builder.appName(nombre_app)
        .master("local[*]")                                    # todos los núcleos lógicos
        .config("spark.driver.memory", memoria_driver)
        .config("spark.sql.shuffle.partitions", str(particiones_shuffle))
        # AQE: re-optimiza el plan con estadísticas reales en tiempo de ejecución
        # (fusiona particiones pequeñas tras el shuffle y puede cambiar el tipo de join).
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Tablas de hasta 64 MB se difunden (broadcast) automáticamente; productos y
        # tiendas se difunden además de forma explícita en joins.py.
        .config("spark.sql.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))
        # Las fechas del CSV no traen zona horaria: se interpretan tal cual, sin
        # conversiones, igual que en pandas (timestamps "naive").
        .config("spark.sql.session.timeZone", "UTC")
        # Parquet con compresión snappy (igual que la versión pandas).
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Evita que la Spark UI conserve demasiado historial en memoria en un equipo de 8 GB.
        .config("spark.ui.retainedJobs", "200")
        .config("spark.ui.retainedStages", "200")
        # Sin barras de progreso en consola: dejan los logs de evidencia ilegibles.
        .config("spark.ui.showConsoleProgress", "false")
    )
    if dir_local:
        constructor = constructor.config("spark.local.dir", dir_local)

    spark = constructor.getOrCreate()
    spark.sparkContext.setLogLevel(nivel_log)
    return spark


def describir_configuracion(spark: SparkSession) -> dict[str, str]:
    """Parámetros efectivos de la sesión (para registrar en logs y evidencias)."""
    conf = spark.sparkContext.getConf()
    claves = [
        "spark.master",
        "spark.driver.memory",
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled",
        "spark.sql.autoBroadcastJoinThreshold",
        "spark.local.dir",
    ]
    info = {k: conf.get(k, spark.conf.get(k, "(por defecto)")) for k in claves}
    info["spark.version"] = spark.version
    info["paralelismo_por_defecto"] = str(spark.sparkContext.defaultParallelism)
    return info
