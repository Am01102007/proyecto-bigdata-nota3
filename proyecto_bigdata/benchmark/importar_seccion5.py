"""
benchmark/importar_seccion5.py
==============================
Convierte las mediciones de la sección 5 del informe (Tabla 10, §5.2 y Figura 7) al formato de
`medir.py`, para generar el §6 mientras no se haya ejecutado el benchmark con 3 repeticiones.

Son UNA ejecución por combinación y no registran CPU. Cuando se ejecute
`python benchmark/medir.py --desde-cero`, estas filas se archivan y se reemplazan por las
mediciones repetidas.

Uso:
    python benchmark/importar_seccion5.py
    python benchmark/graficos.py && python informe/generar_secciones.py
"""

from __future__ import annotations

import json
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
DIR = RAIZ / "evidencias" / "benchmark"

MUESTRAS = {  # pct: (archivos, filas, gb_csv)
    1.0: (1, 1_235_843, 0.115),
    10.0: (10, 12_358_430, 1.153),
    50.0: (50, 61_792_150, 5.766),
    100.0: (100, 123_584_300, 11.532),
}
# (versión, pct): (estado, duración s, pico RAM GB, detalle) - valores de la Tabla 10 y de §5.2
TABLA_10 = {
    ("pandas_directo", 1.0): ("OK", 12.76, 1.20, None),
    ("pandas_mitigado", 1.0): ("OK", 7.52, 0.72, None),
    ("spark", 1.0): ("OK", 18.20, 1.45, None),
    ("pandas_directo", 10.0): ("OK", 112.40, 4.15, None),
    ("pandas_mitigado", 10.0): ("OK", 58.30, 1.85, None),
    ("spark", 10.0): ("OK", 34.50, 2.10, None),
    ("pandas_directo", 50.0): ("MEMORY_ERROR", 140.0, None, "MemoryError tras ≈140 s"),
    ("pandas_mitigado", 50.0): ("OK", 385.60, 4.95, "Resultado no exacto (deduplicación local)"),
    ("spark", 50.0): ("OK", 92.10, 3.80, None),
    ("pandas_directo", 100.0): ("MEMORY_ERROR", 212.54, None, "MemoryError, código de salida 137 (§5.2)"),
    ("pandas_mitigado", 100.0): ("MEMORY_ERROR", 5400.0, None, "Más de 1,5 h y falla de memoria en los conjuntos de clientes"),
    ("spark", 100.0): ("OK", 178.40, 4.60, None),
}


def main() -> None:
    DIR.mkdir(parents=True, exist_ok=True)
    destino = DIR / "mediciones.jsonl"
    if destino.exists():
        raise SystemExit(f"Ya existe {destino}. No se sobrescriben mediciones; muévalo si quiere reimportar.")
    with open(destino, "w", encoding="utf-8") as f:
        for (version, pct), (estado, dur, ram, detalle) in TABLA_10.items():
            archivos, filas, gb = MUESTRAS[pct]
            f.write(json.dumps({
                "version": version, "muestra_pct": pct, "repeticion": 1, "estado": estado,
                "detalle": detalle, "duracion_seg": dur, "pico_ram_gb": ram,
                "cpu_promedio_pct": None, "nucleos_efectivos": None, "ram_sistema_pico_pct": None,
                "archivos": archivos, "filas": filas, "gb_csv": gb,
                "fuente": "Sección 5 del informe (Tabla 10), una ejecución",
            }, ensure_ascii=False) + "\n")
    equipo = json.loads((RAIZ / "evidencias" / "info_equipo.json").read_text(encoding="utf-8"))
    (DIR / "metadatos.json").write_text(json.dumps({
        "fuente": "seccion5",
        "equipo": equipo,
        "parametros": {"repeticiones": 1, "versiones": ["pandas_directo", "pandas_mitigado", "spark"],
                       "muestras": list(MUESTRAS)},
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Importadas {len(TABLA_10)} mediciones de la sección 5 en {destino}")


if __name__ == "__main__":
    main()
