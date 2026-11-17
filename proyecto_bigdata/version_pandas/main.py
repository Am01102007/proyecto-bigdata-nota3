"""
version_pandas/main.py
======================
Punto de entrada principal para la ejecución del pipeline de ventas retail con pandas.

Uso:
    python version_pandas/main.py --muestra 1
    python version_pandas/main.py --muestra 10 --mitigado
    python version_pandas/main.py --muestra 50 --mitigado --chunksize 100000
    python version_pandas/main.py --muestra 100 --mitigado
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import psutil

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from comun.muestras import rutas_muestra, resumen_muestra
from version_pandas.cargador import (
    cargar_dimensiones,
    cargar_transacciones_completas,
)
from version_pandas.limpieza import limpiar_transacciones
from version_pandas.joins import unir_modelo_estrella
from version_pandas.agregaciones import ejecutar_todas_las_consultas
from version_pandas.escritura import guardar_resultados_parquet
from version_pandas.mitigaciones import ejecutar_pipeline_mitigado_por_chunks


def medir_memoria_actual_mb() -> float:
    """Devuelve la memoria RSS consumida por el proceso actual en MB."""
    proceso = psutil.Process(os.getpid())
    return proceso.memory_info().rss / (1024 * 1024)


def ejecutar_pipeline_directo(
    rutas: list[Path],
    dimensiones: dict,
) -> dict:
    """Ejecuta el flujo completo en memoria (sin mitigaciones).

    Carga todas las partes, limpia, ejecuta los 3 joins y las 4 agregaciones.
    En muestras de 50 % o 100 % (o incluso 10 % en equipos de 8 GB), provocará
    una presión extrema de memoria o MemoryError.
    """
    print("  -> [1/4] Carga de transacciones completas en memoria...")
    df_trans = cargar_transacciones_completas(rutas, mitigado=False)
    print(f"     Filas cargadas: {len(df_trans):,} | Memoria: {medir_memoria_actual_mb():.1f} MB")

    print("  -> [2/4] Limpieza y normalización de transacciones...")
    df_limpio = limpiar_transacciones(df_trans, deduplicar=True)
    print(f"     Filas tras limpieza: {len(df_limpio):,} | Memoria: {medir_memoria_actual_mb():.1f} MB")

    print("  -> [3/4] Cruces con dimensiones (productos, tiendas, clientes)...")
    df_enriquecido = unir_modelo_estrella(df_limpio, dimensiones, incluir_clientes=True)
    print(f"     Filas enriquecidas: {len(df_enriquecido):,} | Memoria: {medir_memoria_actual_mb():.1f} MB")

    print("  -> [4/4] Ejecución de las 4 consultas analíticas...")
    resultados = ejecutar_todas_las_consultas(df_enriquecido)
    return resultados


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline de analítica retail con pandas (Versión Samuel - Proyecto Big Data)"
    )
    parser.add_argument(
        "--muestra",
        type=float,
        default=10.0,
        choices=[1.0, 10.0, 50.0, 100.0],
        help="Porcentaje de datos a procesar (1, 10, 50 o 100 %%)",
    )
    parser.add_argument(
        "--mitigado",
        action="store_true",
        help="Aplica técnicas de mitigación: chunksize, dtypes compactos y usecols",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=250_000,
        help="Número de filas por chunk para la versión mitigada (default: 250,000)",
    )
    parser.add_argument(
        "--guardar-parquet",
        action="store_true",
        default=True,
        help="Persiste los resultados agregados en archivos Parquet (default: True)",
    )
    args = parser.parse_args()

    print("=" * 70)
    print(f"PROYECTO NOTA 3: BIG DATA - VERSIÓN PANDAS (Samuel)")
    print(f"Muestra: {args.muestra} % | Modo: {'MITIGADO (chunks + dtypes)' if args.mitigado else 'DIRECTO (en memoria)'}")
    print("=" * 70)

    # Identificar archivos correspondientes a la muestra
    try:
        rutas = rutas_muestra(args.muestra)
        info_m = resumen_muestra(args.muestra)
        print(f"Archivos: {len(rutas)} partes | Filas estimadas: {info_m['filas']:,} | Volumen CSV: {info_m['gb']} GB")
    except Exception as e:
        print(f"Error al obtener la muestra: {e}")
        sys.exit(1)

    t_inicio = time.perf_counter()
    mem_inicial_mb = medir_memoria_actual_mb()

    # Cargar maestros dimensionales
    print("\nCargando dimensiones maestras...")
    dimensiones = cargar_dimensiones(mitigado=args.mitigado)
    print(f"  Dimensiones listas. Memoria actual: {medir_memoria_actual_mb():.1f} MB")

    try:
        if args.mitigado:
            print(f"\nIniciando procesamiento mitigado por chunks (chunksize={args.chunksize:,})...")
            resultados = ejecutar_pipeline_mitigado_por_chunks(
                rutas=rutas,
                dimensiones=dimensiones,
                chunksize=args.chunksize,
            )
        else:
            print("\nIniciando procesamiento directo en memoria (sin mitigaciones)...")
            resultados = ejecutar_pipeline_directo(
                rutas=rutas,
                dimensiones=dimensiones,
            )

        t_fin = time.perf_counter()
        duracion_seg = t_fin - t_inicio
        mem_final_mb = medir_memoria_actual_mb()

        # Guardar en Parquet si se indicó
        if args.guardar_parquet:
            print("\nGuardando resultados analíticos en formato Parquet...")
            rutas_parq = guardar_resultados_parquet(resultados)
            for k, p in rutas_parq.items():
                print(f"  [OK] {k} -> {p}")

        # Resumen de resultados
        print("\n" + "-" * 70)
        print("RESUMEN DE EJECUCIÓN EXITOSA:")
        print(f"  Tiempo total:      {duracion_seg:.2f} segundos ({duracion_seg / 60:.2f} min)")
        print(f"  Memoria inicial:   {mem_inicial_mb:.1f} MB")
        print(f"  Memoria final:     {mem_final_mb:.1f} MB")
        print(f"  Incremento neto:   {mem_final_mb - mem_inicial_mb:.1f} MB")
        print("-" * 70)

        # Imprimir primeras filas de cada consulta para verificación
        for nombre, df in resultados.items():
            print(f"\n>>> Muestra de {nombre} ({len(df):,} filas calculadas):")
            print(df.head(3).to_string())

    except MemoryError as me:
        t_fin = time.perf_counter()
        duracion_seg = t_fin - t_inicio
        print("\n" + "!" * 70)
        print("FALLA CRÍTICA POR MEMORYERROR:")
        print(f"  El proceso agotó la memoria RAM disponible ({psutil.virtual_memory().total / (1024**3):.1f} GB total).")
        print(f"  Tiempo hasta el colapso: {duracion_seg:.2f} s")
        print(f"  Causa: pandas requiere mantener estructuras en memoria sin particionado distribuido ni spill a disco.")
        print("!" * 70)
        sys.exit(137)
    except Exception as exc:
        print(f"\nError inesperado durante la ejecución: {exc}")
        raise


if __name__ == "__main__":
    main()
