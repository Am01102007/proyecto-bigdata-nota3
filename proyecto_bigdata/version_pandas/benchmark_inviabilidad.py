"""
version_pandas/benchmark_inviabilidad.py
========================================
Suite de pruebas automatizadas y benchmarking para evaluar la viabilidad de pandas
frente a volúmenes crecientes de datos (1 %, 10 %, 50 %, 100 %).

Cumple con todos los requisitos del enunciado (§4 y §5):
  - Ejecución en el mismo equipo bajo condiciones idénticas.
  - Evaluación comparativa: sin mitigaciones vs. con mitigaciones (chunks + dtypes + usecols).
  - Medición objetiva de tiempo de ejecución (s), pico de memoria RAM (MB / GB) y CPU %.
  - Repetición de mediciones para rigor estadístico.
  - Registro de fallos por desbordamiento de memoria (MemoryError) y degradación de rendimiento.
  - Exportación de resultados a evidencias/metricas_pandas.json.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any

import psutil

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from comun.muestras import PORCENTAJES_ESCALABILIDAD, rutas_muestra, resumen_muestra
from version_pandas.cargador import cargar_dimensiones, cargar_transacciones_completas
from version_pandas.limpieza import limpiar_transacciones
from version_pandas.joins import unir_modelo_estrella
from version_pandas.agregaciones import ejecutar_todas_las_consultas
from version_pandas.mitigaciones import ejecutar_pipeline_mitigado_por_chunks


class MonitorMemoria(threading.Thread):
    """Hilo de sondeo en segundo plano para registrar el pico exacto de memoria RAM (RSS) y CPU."""

    def __init__(self, intervalo_seg: float = 0.1):
        super().__init__(daemon=True)
        self.intervalo = intervalo_seg
        self.proceso = psutil.Process(os.getpid())
        self.pico_rss_bytes = 0
        self.pico_cpu_pct = 0.0
        self.detener_evento = threading.Event()

    def run(self) -> None:
        while not self.detener_evento.is_set():
            try:
                mem = self.proceso.memory_info().rss
                cpu = self.proceso.cpu_percent(interval=None)
                if mem > self.pico_rss_bytes:
                    self.pico_rss_bytes = mem
                if cpu > self.pico_cpu_pct:
                    self.pico_cpu_pct = cpu
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
            time.sleep(self.intervalo)

    def detener(self) -> tuple[float, float]:
        self.detener_evento.set()
        self.join(timeout=2.0)
        pico_mb = round(self.pico_rss_bytes / (1024 * 1024), 2)
        pico_gb = round(self.pico_rss_bytes / (1024**3), 3)
        return pico_mb, self.pico_cpu_pct


def ejecutar_test_individual(
    porcentaje: float,
    mitigado: bool,
    dimensiones: dict,
    chunksize: int = 250_000,
) -> dict[str, Any]:
    """Ejecuta una corrida individual bajo un monitor estricto de recursos."""
    gc.collect()
    time.sleep(1.0)  # Estabilizar memoria

    rutas = rutas_muestra(porcentaje)
    info_m = resumen_muestra(porcentaje)

    monitor = MonitorMemoria(intervalo_seg=0.05)
    monitor.start()

    t_inicio = time.perf_counter()
    estado = "EXITOSO"
    mensaje_error = None
    mem_inicial_mb = round(psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024), 2)

    try:
        if mitigado:
            res = ejecutar_pipeline_mitigado_por_chunks(
                rutas=rutas,
                dimensiones=dimensiones,
                chunksize=chunksize,
            )
        else:
            # Enfoque directo sin mitigaciones
            df_trans = cargar_transacciones_completas(rutas, mitigado=False)
            df_limpio = limpiar_transacciones(df_trans, deduplicar=True)
            df_enriquecido = unir_modelo_estrella(df_limpio, dimensiones, incluir_clientes=True)
            res = ejecutar_todas_las_consultas(df_enriquecido)

    except MemoryError as e:
        estado = "MEMORY_ERROR"
        mensaje_error = "MemoryError: Se superó la capacidad de la memoria RAM del sistema."
    except Exception as e:
        estado = "ERROR"
        mensaje_error = str(e)
    finally:
        t_fin = time.perf_counter()
        pico_rss_mb, pico_cpu = monitor.detener()
        duracion_seg = round(t_fin - t_inicio, 3)

    return {
        "muestra_pct": porcentaje,
        "mitigado": mitigado,
        "archivos_csv": len(rutas),
        "filas_muestra": info_m["filas"],
        "gb_csv": info_m["gb"],
        "estado": estado,
        "duracion_seg": duracion_seg,
        "memoria_inicial_mb": mem_inicial_mb,
        "pico_memoria_mb": pico_rss_mb,
        "pico_memoria_gb": round(pico_rss_mb / 1024, 3),
        "pico_cpu_pct": pico_cpu,
        "error": mensaje_error,
    }


def ejecutar_banco_pruebas(
    muestras: list[float] | None = None,
    repeticiones: int = 1,
    destino_json: Path = RAIZ / "evidencias" / "metricas_pandas.json",
) -> list[dict[str, Any]]:
    """Ejecuta el conjunto sistemático de pruebas de escalabilidad y genera el reporte JSON."""
    if muestras is None:
        muestras = [1.0, 10.0, 50.0, 100.0]

    print("=" * 80)
    print("INICIANDO SUITE DE EVALUACIÓN DE INVIABILIDAD DE PANDAS")
    print(f"Muestras: {muestras} | Repeticiones por caso: {repeticiones}")
    print("=" * 80)

    # Cargar dimensiones una vez para la sesión de benchmark
    print("Cargando maestros dimensionales...")
    dimensiones = cargar_dimensiones(mitigado=True)
    print("Dimensiones listas.\n")

    registros: list[dict[str, Any]] = []

    for pct in muestras:
        for mitigado in [False, True]:
            modo_txt = "CON MITIGACIÓN (chunks+dtypes)" if mitigado else "SIN MITIGACIÓN (directo)"
            print(f"\n>>> Probando Muestra {pct}% [{modo_txt}]")

            for r in range(1, repeticiones + 1):
                print(f"    Intento {r}/{repeticiones}...", end=" ", flush=True)
                resultado = ejecutar_test_individual(
                    porcentaje=pct,
                    mitigado=mitigado,
                    dimensiones=dimensiones,
                )
                resultado["repeticion"] = r
                registros.append(resultado)

                if resultado["estado"] == "EXITOSO":
                    print(f"[OK] Tiempo: {resultado['duracion_seg']} s | Pico RAM: {resultado['pico_memoria_gb']} GB")
                elif resultado["estado"] == "MEMORY_ERROR":
                    print(f"[FALLA] MemoryError tras {resultado['duracion_seg']} s (RAM agotada)")
                else:
                    print(f"[ERROR] {resultado['error']}")

                # Si ya colapsó en 50% directo, no repetir innecesariamente 100% directo
                if not mitigado and resultado["estado"] == "MEMORY_ERROR":
                    print("    -> Nota: La versión sin mitigaciones demostró inviabilidad absoluta para este volumen.")
                    break

    # Guardar resultados
    destino_json.parent.mkdir(parents=True, exist_ok=True)
    destino_json.write_text(json.dumps(registros, indent=2, ensure_ascii=False), encoding="utf-8")
    print("\n" + "=" * 80)
    print(f"Métricas exportadas exitosamente a: {destino_json}")
    print("=" * 80)

    return registros


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark de inviabilidad para pandas")
    parser.add_argument("--muestras", nargs="+", type=float, default=[1.0, 10.0, 50.0, 100.0])
    parser.add_argument("--repeticiones", type=int, default=1)
    args = parser.parse_args()

    ejecutar_banco_pruebas(muestras=args.muestras, repeticiones=args.repeticiones)
