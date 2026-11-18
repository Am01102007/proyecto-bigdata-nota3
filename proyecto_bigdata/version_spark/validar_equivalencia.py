"""
version_spark/validar_equivalencia.py
=====================================
Comprueba que la versión PySpark y la versión pandas producen los MISMOS resultados.

Procedimiento: ejecute ambas versiones con la misma muestra y luego este script.

    python version_pandas/main.py --muestra 1
    python version_spark/main.py  --muestra 1
    python version_spark/validar_equivalencia.py --etiqueta "muestra 1 %"

Para cada consulta une los dos resultados por sus columnas clave y compara cada
métrica:
  - Conteos (enteros): deben ser idénticos.
  - Montos (decimales): diferencia relativa <= 1e-4. pandas usa float32 (~7 cifras
    significativas, mitigación de memoria) y Spark float64; la diferencia es de redondeo.
  - Porcentajes y ticket redondeados a 2 decimales: diferencia absoluta <= 0,01 más
    2 "pasos" de float32 (2 x 1,19e-7 x valor). Un float32 no puede representar 2
    decimales en valores grandes: cerca de 87 000 solo distingue saltos de ~0,008, por
    eso pandas guarda 87223.796875 donde Spark guarda 87223.81.

El informe se imprime y se guarda en evidencias/equivalencia_pandas_spark.md.
Lee los resultados con pandas/pyarrow, sin Spark (son tablas pequeñas).
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]

# consulta -> (claves, métricas enteras, métricas decimales, métricas redondeadas a 2 dec.)
CONSULTAS = {
    "pregunta_1_ventas_margen": (
        ["anio_mes", "region", "categoria"],
        ["unidades_vendidas"],
        ["venta_neta_total", "margen_bruto_total"],
        ["margen_pct"],
    ),
    "pregunta_2_tasas_devolucion_cancelacion": (
        ["canal", "categoria"],
        ["total_transacciones", "transacciones_devueltas", "transacciones_canceladas"],
        [],
        ["tasa_devolucion_pct", "tasa_cancelacion_pct"],
    ),
    "pregunta_3_top10_productos": (
        ["departamento", "producto_id"],
        ["unidades_vendidas", "ranking"],
        [],
        [],
    ),
    "pregunta_4_ticket_clientes": (
        ["segmento", "anio_mes"],
        ["clientes_unicos", "total_transacciones"],
        ["venta_total"],
        ["ticket_promedio"],
    ),
}
RTOL = 1e-4
ATOL_REDONDEO = 0.0101
EPS_FLOAT32 = float(np.finfo(np.float32).eps)  # 1,19e-7: precisión relativa de float32


def leer(ruta: Path) -> pd.DataFrame:
    """Lee un .parquet (pandas) o una carpeta de part-*.parquet (Spark)."""
    df = pd.read_parquet(ruta)
    # La versión pandas mitigada nombra 'cantidad' a las unidades de la consulta 3.
    if "unidades_vendidas" not in df.columns and "cantidad" in df.columns:
        df = df.rename(columns={"cantidad": "unidades_vendidas"})
    for c in df.columns:  # category/string -> str para poder unir por claves
        if not pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].astype(str)
    return df


def comparar_consulta(nombre: str, dir_pandas: Path, dir_spark: Path) -> dict:
    claves, enteras, decimales, redondeadas = CONSULTAS[nombre]
    pdf = leer(dir_pandas / f"{nombre}.parquet")
    sdf = leer(dir_spark / nombre)
    unido = pdf.merge(sdf, on=claves, how="outer", suffixes=("_pd", "_sp"), indicator=True)

    res = {
        "consulta": nombre,
        "filas_pandas": len(pdf),
        "filas_spark": len(sdf),
        "solo_pandas": int((unido["_merge"] == "left_only").sum()),
        "solo_spark": int((unido["_merge"] == "right_only").sum()),
        "columnas": [],
    }
    ambos = unido[unido["_merge"] == "both"]

    def registrar(col: str, distintas: pd.Series, max_dif: float, criterio: str) -> None:
        n = int(distintas.sum())
        ejemplos = [" / ".join(map(str, fila))
                    for fila in ambos.loc[distintas, claves].head(3).itertuples(index=False)]
        res["columnas"].append({"columna": col, "criterio": criterio, "filas_distintas": n,
                                "max_diferencia": max_dif, "ejemplos": ejemplos})

    for c in enteras:
        a, b = ambos[f"{c}_pd"].astype("int64"), ambos[f"{c}_sp"].astype("int64")
        dif = (a - b).abs()
        registrar(c, dif > 0, float(dif.max() if len(dif) else 0), "idéntico")
    for c in decimales:
        a, b = ambos[f"{c}_pd"].astype("float64"), ambos[f"{c}_sp"].astype("float64")
        rel = (a - b).abs() / np.maximum(b.abs(), 1e-9)
        registrar(c, rel > RTOL, float(rel.max() if len(rel) else 0), f"dif. relativa <= {RTOL:g}")
    for c in redondeadas:
        a, b = ambos[f"{c}_pd"].astype("float64"), ambos[f"{c}_sp"].astype("float64")
        dif = (a - b).abs()
        tolerancia = ATOL_REDONDEO + 2 * EPS_FLOAT32 * b.abs()
        registrar(c, dif > tolerancia, float(dif.max() if len(dif) else 0),
                  "dif. absoluta <= 0,01 + 2 pasos float32")

    res["equivalente"] = (
        res["solo_pandas"] == 0 and res["solo_spark"] == 0
        and all(c["filas_distintas"] == 0 for c in res["columnas"])
    )
    return res


def informe_markdown(resultados: list[dict], etiqueta: str) -> str:
    lineas = [
        f"# Equivalencia de resultados pandas vs. PySpark ({etiqueta})",
        "",
        f"Generado: {datetime.now().isoformat(timespec='seconds')}",
        "",
        "| Consulta | Filas pandas | Filas Spark | Claves solo en una | Resultado |",
        "|---|---|---|---|---|",
    ]
    for r in resultados:
        estado = "EQUIVALENTE" if r["equivalente"] else "DIFERENCIAS"
        lineas.append(f"| {r['consulta']} | {r['filas_pandas']:,} | {r['filas_spark']:,} | "
                      f"{r['solo_pandas'] + r['solo_spark']} | **{estado}** |")
    lineas += ["", "## Detalle por métrica", ""]
    for r in resultados:
        lineas += [f"### {r['consulta']}", "",
                   "| Métrica | Criterio | Filas distintas | Diferencia máxima | Ejemplos (claves) |",
                   "|---|---|---|---|---|"]
        for c in r["columnas"]:
            lineas.append(f"| {c['columna']} | {c['criterio']} | {c['filas_distintas']} | "
                          f"{c['max_diferencia']:.6g} | {'; '.join(c['ejemplos'])} |")
        lineas.append("")
    return "\n".join(lineas)


def main() -> None:
    ap = argparse.ArgumentParser(description="Valida que pandas y Spark den los mismos resultados")
    ap.add_argument("--pandas", type=Path, default=RAIZ / "resultados" / "pandas")
    ap.add_argument("--spark", type=Path, default=RAIZ / "resultados" / "spark")
    ap.add_argument("--etiqueta", default="", help="Texto para el título, p. ej. 'muestra 1 %%'")
    ap.add_argument("--salida", type=Path, default=RAIZ / "evidencias" / "equivalencia_pandas_spark.md")
    args = ap.parse_args()

    resultados = [comparar_consulta(n, args.pandas, args.spark) for n in CONSULTAS]
    texto = informe_markdown(resultados, args.etiqueta or "sin etiqueta")
    args.salida.parent.mkdir(parents=True, exist_ok=True)
    args.salida.write_text(texto, encoding="utf-8")
    print(texto)
    print(f"\nInforme guardado en {args.salida}")
    sys.exit(0 if all(r["equivalente"] for r in resultados) else 1)


if __name__ == "__main__":
    main()
