"""
benchmark/complejidad_codigo.py
===============================
Métricas objetivas de complejidad del código de ambas versiones (enunciado §2.5:
"comparar ... complejidad del código"). Solo usa la librería estándar (ast, tokenize).

Por cada módulo del pipeline (se excluyen pruebas, validadores y benchmarks):
  - LOC: líneas de código efectivas (sin líneas en blanco, comentarios ni docstrings).
  - Funciones definidas y LOC promedio por función.
  - Complejidad ciclomática de McCabe por función: 1 + puntos de decisión
    (if/elif, for, while, except, and/or, expresiones condicionales y filtros de
    comprensiones). Se reporta la suma y el máximo.
  - Profundidad máxima de anidamiento de bloques.

Uso:
    python benchmark/complejidad_codigo.py
Salida:
    evidencias/benchmark/complejidad_codigo.json
    evidencias/benchmark/complejidad_codigo.md
"""

from __future__ import annotations

import argparse
import ast
import io
import json
import tokenize
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
DIR_SALIDA = RAIZ / "evidencias" / "benchmark"

# Etapa del pipeline -> módulo de cada versión (None = no existe en esa versión)
ETAPAS = [
    ("Configuración del motor", None, "sesion.py"),
    ("Carga", "cargador.py", "cargador.py"),
    ("Limpieza", "limpieza.py", "limpieza.py"),
    ("Joins", "joins.py", "joins.py"),
    ("Agregaciones (4 consultas)", "agregaciones.py", "agregaciones.py"),
    ("Escritura Parquet", "escritura.py", "escritura.py"),
    ("Mitigación por chunks", "mitigaciones.py", None),
    ("Orquestación (CLI)", "main.py", "main.py"),
]
NODOS_BLOQUE = (ast.If, ast.For, ast.AsyncFor, ast.While, ast.With, ast.AsyncWith, ast.Try)


def lineas_efectivas(codigo: str) -> int:
    """Líneas con código ejecutable: sin blancos, comentarios ni docstrings."""
    arbol = ast.parse(codigo)
    docstrings: set[int] = set()
    for nodo in ast.walk(arbol):
        if isinstance(nodo, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            cuerpo = nodo.body
            if cuerpo and isinstance(cuerpo[0], ast.Expr) and isinstance(getattr(cuerpo[0], "value", None),
                                                                           ast.Constant) \
                    and isinstance(cuerpo[0].value.value, str):
                docstrings.update(range(cuerpo[0].lineno, cuerpo[0].end_lineno + 1))
    con_codigo: set[int] = set()
    for tok in tokenize.generate_tokens(io.StringIO(codigo).readline):
        if tok.type in (tokenize.COMMENT, tokenize.NL, tokenize.NEWLINE, tokenize.INDENT,
                        tokenize.DEDENT, tokenize.ENDMARKER):
            continue
        for linea in range(tok.start[0], tok.end[0] + 1):
            if linea not in docstrings:
                con_codigo.add(linea)
    return len(con_codigo)


def ciclomatica(funcion: ast.AST) -> int:
    total = 1
    for n in ast.walk(funcion):
        if isinstance(n, (ast.If, ast.For, ast.AsyncFor, ast.While, ast.IfExp, ast.ExceptHandler)):
            total += 1
        elif isinstance(n, ast.BoolOp):
            total += len(n.values) - 1
        elif isinstance(n, ast.comprehension):
            total += len(n.ifs)
    return total


def profundidad(nodo: ast.AST, nivel: int = 0) -> int:
    maximo = nivel
    for hijo in ast.iter_child_nodes(nodo):
        siguiente = nivel + 1 if isinstance(hijo, NODOS_BLOQUE) else nivel
        maximo = max(maximo, profundidad(hijo, siguiente))
    return maximo


def medir_archivo(ruta: Path) -> dict:
    codigo = ruta.read_text(encoding="utf-8")
    arbol = ast.parse(codigo)
    funciones = [n for n in ast.walk(arbol) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    cc = [ciclomatica(f) for f in funciones]
    loc_funciones = [lineas_efectivas(ast.get_source_segment(codigo, f) or "") for f in funciones]
    return {
        "archivo": ruta.name,
        "loc": lineas_efectivas(codigo),
        "lineas_totales": len(codigo.splitlines()),
        "funciones": len(funciones),
        "loc_por_funcion": round(sum(loc_funciones) / len(loc_funciones), 1) if loc_funciones else 0,
        "cc_total": sum(cc),
        "cc_max": max(cc) if cc else 0,
        "funcion_cc_max": funciones[cc.index(max(cc))].name if cc else None,
        "anidamiento_max": profundidad(arbol),
    }


def num(x: float, dec: int = 0) -> str:
    return f"{x:,.{dec}f}".replace(",", "§").replace(".", ",").replace("§", ".")


def main() -> None:
    ap = argparse.ArgumentParser(description="Métricas de complejidad de código pandas vs. Spark")
    ap.add_argument("--tabla", type=int, default=14, help="Número de la tabla en el informe")
    args = ap.parse_args()

    resultado: dict = {"etapas": [], "totales": {}}
    for version, carpeta, col in (("pandas", "version_pandas", 1), ("spark", "version_spark", 2)):
        tot = {"archivos": 0, "loc": 0, "funciones": 0, "cc_total": 0, "cc_max": 0, "anidamiento_max": 0}
        for etapa in ETAPAS:
            if etapa[col] and (RAIZ / carpeta / etapa[col]).exists():
                m = medir_archivo(RAIZ / carpeta / etapa[col])
                tot["archivos"] += 1
                for k in ("loc", "funciones", "cc_total"):
                    tot[k] += m[k]
                tot["cc_max"] = max(tot["cc_max"], m["cc_max"])
                tot["anidamiento_max"] = max(tot["anidamiento_max"], m["anidamiento_max"])
        tot["cc_media_por_funcion"] = round(tot["cc_total"] / tot["funciones"], 2) if tot["funciones"] else 0
        resultado["totales"][version] = tot
    for nombre, f_pandas, f_spark in ETAPAS:
        fila = {"etapa": nombre}
        for version, carpeta, archivo in (("pandas", "version_pandas", f_pandas), ("spark", "version_spark", f_spark)):
            ruta = RAIZ / carpeta / archivo if archivo else None
            fila[version] = medir_archivo(ruta) if ruta and ruta.exists() else None
        resultado["etapas"].append(fila)

    def celda(m: dict | None) -> str:
        return "-" if not m else f"{m['loc']} / {m['funciones']} / {m['cc_max']}"

    lineas = [f"**Tabla {args.tabla}.** *Complejidad del código por etapa del pipeline: líneas de código "
              "efectivas / funciones / complejidad ciclomática máxima de una función*\n",
              "| Etapa | pandas | PySpark |", "|---|---|---|"]
    for fila in resultado["etapas"]:
        lineas.append(f"| {fila['etapa']} | {celda(fila['pandas'])} | {celda(fila['spark'])} |")
    tp, ts = resultado["totales"]["pandas"], resultado["totales"]["spark"]
    lineas.append(f"| **Total** | **{num(tp['loc'])} / {tp['funciones']} / {tp['cc_max']}** | "
                  f"**{num(ts['loc'])} / {ts['funciones']} / {ts['cc_max']}** |")
    lineas.append(f"| Complejidad ciclomática media por función | {num(tp['cc_media_por_funcion'], 2)} | "
                  f"{num(ts['cc_media_por_funcion'], 2)} |")
    lineas.append(f"| Anidamiento máximo de bloques | {tp['anidamiento_max']} | {ts['anidamiento_max']} |")
    lineas.append("\nLíneas efectivas: sin líneas en blanco, comentarios ni docstrings. Complejidad "
                  "ciclomática de McCabe: 1 + número de decisiones (if, bucles, except, and/or). "
                  "Se excluyen los scripts de prueba, validación y benchmark. "
                  "Medido con `benchmark/complejidad_codigo.py`.\n")

    DIR_SALIDA.mkdir(parents=True, exist_ok=True)
    (DIR_SALIDA / "complejidad_codigo.json").write_text(json.dumps(resultado, indent=2, ensure_ascii=False),
                                                         encoding="utf-8")
    (DIR_SALIDA / "complejidad_codigo.md").write_text("\n".join(lineas), encoding="utf-8")
    print("\n".join(lineas))


if __name__ == "__main__":
    main()
