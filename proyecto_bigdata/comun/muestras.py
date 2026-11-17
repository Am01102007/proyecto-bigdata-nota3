"""
comun/muestras.py
=================
Selección de muestras COMPARTIDA por la versión pandas y la versión Spark.

La tabla de hechos está dividida en N archivos (part-00000.csv ...). Una muestra
del X % son los primeros round(N * X / 100) archivos. Así ambas versiones
procesan exactamente las mismas filas en cada experimento de escalabilidad.

Uso en pandas:
    from comun.muestras import rutas_muestra
    for ruta in rutas_muestra(10):        # 10 %
        for chunk in pd.read_csv(ruta, chunksize=1_000_000): ...

Uso en PySpark:
    from comun.muestras import rutas_muestra
    df = spark.read.csv([str(p) for p in rutas_muestra(10)], header=True, schema=ESQUEMA)
"""

from __future__ import annotations

import json
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
DATOS_RAW = RAIZ / "datos" / "raw"
DIR_TRANSACCIONES = DATOS_RAW / "transacciones"
DIR_DIMENSIONES = DATOS_RAW / "dimensiones"

PORCENTAJES_ESCALABILIDAD = [1, 10, 50, 100]


def rutas_muestra(porcentaje: float, directorio: Path = DIR_TRANSACCIONES) -> list[Path]:
    """Devuelve la lista ordenada de archivos que forman la muestra del `porcentaje` %."""
    partes = sorted(Path(directorio).glob("part-*.csv"))
    if not partes:
        raise FileNotFoundError(
            f"No hay archivos part-*.csv en {directorio}. Ejecute primero scripts/generar_datos.py"
        )
    k = max(1, round(len(partes) * porcentaje / 100))
    return partes[:k]


def resumen_muestra(porcentaje: float) -> dict:
    """Filas y bytes de una muestra, leídos del manifiesto (sin abrir los CSV)."""
    manifiesto = json.loads((DATOS_RAW / "manifiesto.json").read_text(encoding="utf-8"))
    detalle = manifiesto["transacciones"]["detalle_partes"]
    k = len(rutas_muestra(porcentaje))
    sel = detalle[:k]
    return {
        "porcentaje": porcentaje,
        "archivos": k,
        "filas": sum(p["filas"] for p in sel),
        "gb": round(sum(p["bytes"] for p in sel) / 1024**3, 3),
    }


if __name__ == "__main__":
    for p in PORCENTAJES_ESCALABILIDAD:
        try:
            print(resumen_muestra(p))
        except FileNotFoundError as e:
            print(e)
            break
