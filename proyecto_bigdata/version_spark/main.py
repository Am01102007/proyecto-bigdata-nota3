"""
version_spark/main.py
=====================
Punto de entrada del pipeline de ventas retail con PySpark.

Uso (desde la raíz del proyecto, con el entorno virtual activado):
    python version_spark/main.py --muestra 1
    python version_spark/main.py --muestra 100
    python version_spark/main.py --muestra 10 --explain --perfil
    python version_spark/main.py --muestra 100 --guardar-base --memoria-driver 4g

Para el benchmark (otro script) se puede importar `ejecutar_pipeline`, que recibe una
SparkSession ya creada y devuelve los tiempos por etapa:

    from version_spark.sesion import crear_sesion
    from version_spark.main import ejecutar_pipeline
    spark = crear_sesion()
    metricas = ejecutar_pipeline(spark, porcentaje=10)

Medición de tiempos por etapa: por la evaluación perezosa, las transformaciones no
cuestan nada hasta que una ACCIÓN las ejecuta. Por eso cada etapa termina en una
acción explícita: `count()` materializa la base en caché y cada `write` ejecuta una
consulta.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import time
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from comun.muestras import resumen_muestra, rutas_muestra  # noqa: E402
from version_spark.agregaciones import ejecutar_todas_las_consultas, preparar_base  # noqa: E402
from version_spark.cargador import cargar_dimensiones, cargar_transacciones  # noqa: E402
from version_spark.escritura import (  # noqa: E402
    DIR_RESULTADOS_SPARK,
    guardar_base_particionada,
    guardar_resultado,
)
from version_spark.joins import unir_modelo_estrella  # noqa: E402
from version_spark.limpieza import limpiar_transacciones, perfil_calidad  # noqa: E402
from version_spark.sesion import crear_sesion, describir_configuracion  # noqa: E402


def plan_como_texto(df: DataFrame, modo: str = "formatted") -> str:
    """Devuelve el plan de ejecución (lo que imprime df.explain) como texto."""
    salida = io.StringIO()
    with contextlib.redirect_stdout(salida):
        df.explain(mode=modo)
    return salida.getvalue()


def ejecutar_pipeline(
    spark: SparkSession,
    porcentaje: float,
    usar_cache: bool = True,
    guardar_base: bool = False,
    dir_salida: Path = DIR_RESULTADOS_SPARK,
    dir_planes: Path | None = None,
    perfil: bool = False,
    verbose: bool = True,
) -> dict:
    """Ejecuta lectura -> limpieza -> joins -> 4 consultas -> Parquet. Devuelve métricas."""
    log = print if verbose else (lambda *a, **k: None)
    tiempos: dict[str, float] = {}
    t_total = time.perf_counter()

    # 1. Plan de lectura, limpieza y joins (perezoso: aquí no se lee nada)
    tx = cargar_transacciones(spark, porcentaje)
    dims = cargar_dimensiones(spark)

    calidad = None
    if perfil:
        log("  -> Perfil de calidad del CSV crudo (pasada extra, solo con --perfil)...")
        t = time.perf_counter()
        calidad = perfil_calidad(tx)
        tiempos["perfil_calidad"] = time.perf_counter() - t
        log(f"     {calidad}")

    base = preparar_base(unir_modelo_estrella(limpiar_transacciones(tx), dims))

    # 2. Materializar la base: lectura CSV + dedupe global + limpieza + broadcast joins.
    #    MEMORY_AND_DISK: lo que no quepa en RAM se guarda en disco local, no se recalcula.
    filas_base = None
    if usar_cache:
        log("  -> [1/3] Lectura, limpieza, joins y caché de la base (MEMORY_AND_DISK)...")
        base = base.persist(StorageLevel.MEMORY_AND_DISK)
        t = time.perf_counter()
        filas_base = base.count()
        tiempos["preparacion_base"] = time.perf_counter() - t
        log(f"     Filas limpias y enriquecidas: {filas_base:,} ({tiempos['preparacion_base']:.1f} s)")

    # 3. Consultas: cada write es una acción que ejecuta su plan
    log("  -> [2/3] Ejecución de las 4 consultas y escritura en Parquet...")
    consultas = ejecutar_todas_las_consultas(base, dims["clientes"], spark)
    salidas: dict[str, dict] = {}
    for nombre, df in consultas.items():
        if dir_planes is not None:
            dir_planes.mkdir(parents=True, exist_ok=True)
            (dir_planes / f"plan_{nombre}.txt").write_text(plan_como_texto(df), encoding="utf-8")
        t = time.perf_counter()
        ruta = guardar_resultado(df, nombre, dir_salida)
        tiempos[nombre] = time.perf_counter() - t
        salidas[nombre] = {"ruta": str(ruta), "filas": spark.read.parquet(str(ruta)).count()}
        log(f"     {nombre}: {salidas[nombre]['filas']:,} filas ({tiempos[nombre]:.1f} s)")

    # 4. Opcional: base limpia en Parquet particionado por año/mes
    if guardar_base:
        log("  -> [3/3] Escritura de la base limpia particionada por anio/mes...")
        t = time.perf_counter()
        ruta_base = guardar_base_particionada(base, dir_salida / "ventas_limpias")
        tiempos["escritura_base_particionada"] = time.perf_counter() - t
        log(f"     {ruta_base} ({tiempos['escritura_base_particionada']:.1f} s)")

    if dir_planes is not None:
        (dir_planes / "plan_base.txt").write_text(plan_como_texto(base), encoding="utf-8")

    if usar_cache:
        base.unpersist()

    tiempos["total_pipeline"] = time.perf_counter() - t_total
    return {
        "muestra_pct": porcentaje,
        "usar_cache": usar_cache,
        "filas_base": filas_base,
        "tiempos_seg": {k: round(v, 3) for k, v in tiempos.items()},
        "salidas": salidas,
        "perfil_calidad": calidad,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline de analítica retail con PySpark")
    parser.add_argument("--muestra", type=float, default=10.0,
                        help="Porcentaje de datos: 1, 10, 50 o 100 (default: 10)")
    parser.add_argument("--memoria-driver", default=None,
                        help="spark.driver.memory, p. ej. 4g (default: ~50%% de la RAM)")
    parser.add_argument("--particiones", type=int, default=None,
                        help="spark.sql.shuffle.partitions (default: 3 x núcleos)")
    parser.add_argument("--sin-cache", action="store_true",
                        help="No guarda la base en caché (para medir el efecto del caching)")
    parser.add_argument("--guardar-base", action="store_true",
                        help="Escribe además la base limpia en Parquet particionado por anio/mes")
    parser.add_argument("--explain", action="store_true",
                        help="Guarda los planes de ejecución en evidencias/planes_spark/")
    parser.add_argument("--perfil", action="store_true",
                        help="Cuenta los problemas de calidad del CSV crudo (pasada extra)")
    args = parser.parse_args()

    print("=" * 70)
    print("PROYECTO NOTA 3: BIG DATA - VERSIÓN PYSPARK")
    print(f"Muestra: {args.muestra} % | Caché: {'no' if args.sin_cache else 'sí'}")
    print("=" * 70)

    try:
        rutas = rutas_muestra(args.muestra)
        info = resumen_muestra(args.muestra)
        print(f"Archivos: {len(rutas)} partes | Filas: {info['filas']:,} | Volumen CSV: {info['gb']} GB")
    except Exception as e:  # noqa: BLE001
        print(f"Error al obtener la muestra: {e}")
        sys.exit(1)

    t = time.perf_counter()
    spark = crear_sesion(memoria_driver=args.memoria_driver, particiones_shuffle=args.particiones)
    t_sesion = time.perf_counter() - t
    conf = describir_configuracion(spark)
    print(f"SparkSession lista en {t_sesion:.1f} s | " +
          " | ".join(f"{k.replace('spark.', '')}={v}" for k, v in conf.items()
                     if k in ("spark.master", "spark.driver.memory", "spark.sql.shuffle.partitions")))
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}\n")

    try:
        metricas = ejecutar_pipeline(
            spark,
            args.muestra,
            usar_cache=not args.sin_cache,
            guardar_base=args.guardar_base,
            dir_planes=RAIZ / "evidencias" / "planes_spark" if args.explain else None,
            perfil=args.perfil,
        )
        metricas["tiempo_sesion_seg"] = round(t_sesion, 3)
        metricas["configuracion"] = conf
        metricas["fecha"] = datetime.now().isoformat(timespec="seconds")

        resumen = DIR_RESULTADOS_SPARK / "resumen_ejecucion.json"
        resumen.write_text(json.dumps(metricas, indent=2, ensure_ascii=False), encoding="utf-8")

        print("\n" + "-" * 70)
        print("RESUMEN DE EJECUCIÓN EXITOSA:")
        print(f"  Tiempo del pipeline: {metricas['tiempos_seg']['total_pipeline']:.2f} s "
              f"(+ {t_sesion:.1f} s de arranque de la JVM)")
        print(f"  Métricas guardadas en: {resumen}")
        print("-" * 70)

        for nombre, salida in metricas["salidas"].items():
            print(f"\n>>> Muestra de {nombre} ({salida['filas']:,} filas):")
            spark.read.parquet(salida["ruta"]).show(3, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
