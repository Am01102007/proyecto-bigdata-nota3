"""
benchmark/graficos.py
=====================
Convierte las mediciones de `benchmark/medir.py` en las tablas y figuras del §6.

Entrada:  evidencias/benchmark/mediciones.jsonl  (+ metadatos.json)
Salida:   evidencias/benchmark/
            resumen_por_muestra.csv     estadísticos por versión y muestra (n, media, desv., mín., máx.)
            mediciones_detalle.csv      todas las repeticiones, una fila por ejecución
            tablas.md                   tablas listas para el informe (formato español: 1.234,5)
            resumen.json                cifras derivadas que usa informe/generar_secciones.py
            fig_tiempo.png              tiempo vs. muestra (escala log)
            fig_memoria.png             pico de RAM vs. muestra, con la RAM física del equipo
            fig_cpu.png                 núcleos efectivos por versión y muestra

Uso:
    python benchmark/graficos.py
    python benchmark/graficos.py --entrada otra_carpeta/mediciones.jsonl --salida otra_carpeta

Estadística: media y desviación estándar muestral (n-1) de las repeticiones que terminaron
bien. Las barras de error de las figuras van del mínimo al máximo de las repeticiones.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics as st
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.patches  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

RAIZ = Path(__file__).resolve().parents[1]
DIR_BENCH = RAIZ / "evidencias" / "benchmark"

ORDEN = ["pandas_directo", "pandas_mitigado", "spark"]
NOMBRES = {
    "pandas_directo": "pandas directo",
    "pandas_mitigado": "pandas mitigado (chunks)",
    "spark": "PySpark local[*]",
}
# Paleta categórica validada (orden fijo: azul, naranja, aguamarina) y tintas de texto
COLORES = {"pandas_directo": "#2a78d6", "pandas_mitigado": "#eb6834", "spark": "#1baf7a"}
MARCADORES = {"pandas_directo": "o", "pandas_mitigado": "s", "spark": "^"}
TINTA, TINTA_2, REJILLA, SUPERFICIE = "#0b0b0b", "#52514e", "#e4e3de", "#ffffff"
CORTOS = {"pandas_directo": "directo", "pandas_mitigado": "mitigado", "spark": "Spark"}
UNA_EJECUCION = False      # True con las mediciones importadas de la sección 5 (n = 1)
ETIQUETA_ESTADO = {
    "MEMORY_ERROR": "MemoryError",
    "TIMEOUT": "Tiempo límite",
    "ERROR": "Error",
    "OMITIDO": "No ejecutado",
}


# ----------------------------------------------------------------------------
# Formato numérico en español (1.234,5)
# ----------------------------------------------------------------------------
def num(x: float | None, dec: int = 1) -> str:
    if x is None:
        return "-"
    txt = f"{x:,.{dec}f}"
    return txt.replace(",", "§").replace(".", ",").replace("§", ".")


def tiempo_legible(seg: float | None) -> str:
    if seg is None:
        return "-"
    if seg < 120:
        return f"{num(seg, 1)} s"
    if seg < 7200:
        return f"{num(seg / 60, 1)} min"
    return f"{num(seg / 3600, 2)} h"


# ----------------------------------------------------------------------------
# Lectura y agregación
# ----------------------------------------------------------------------------
def leer(ruta: Path) -> list[dict]:
    ultimas: dict[tuple, dict] = {}
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        if linea.strip():
            r = json.loads(linea)
            ultimas[(r["version"], float(r["muestra_pct"]), r["repeticion"])] = r
    return sorted(ultimas.values(), key=lambda r: (float(r["muestra_pct"]), ORDEN.index(r["version"])
                                                   if r["version"] in ORDEN else 99, r["repeticion"]))


def estadisticos(valores: list[float]) -> dict:
    valores = [v for v in valores if v is not None]
    if not valores:
        return {"media": None, "desv": None, "min": None, "max": None}
    return {
        "media": st.mean(valores),
        "desv": st.stdev(valores) if len(valores) > 1 else None,
        "min": min(valores),
        "max": max(valores),
    }


def agregar(registros: list[dict]) -> list[dict]:
    grupos: dict[tuple, list[dict]] = {}
    for r in registros:
        grupos.setdefault((r["version"], float(r["muestra_pct"])), []).append(r)
    filas = []
    for (version, pct), regs in grupos.items():
        ejecutadas = [r for r in regs if r["estado"] != "OMITIDO"]
        ok = [r for r in ejecutadas if r["estado"] == "OK"]
        fallidas = [r for r in ejecutadas if r["estado"] != "OK"]
        if ok:
            estado = "OK" if not fallidas else "PARCIAL"
        elif fallidas:
            # El estado más frecuente entre las fallidas (MemoryError, tiempo límite o error)
            estado = st.mode([r["estado"] for r in fallidas])
        else:
            estado = "OMITIDO"
        base = ok if ok else fallidas
        fila = {
            "version": version, "muestra_pct": pct, "estado": estado,
            "n_total": len(regs), "n_ejecutadas": len(ejecutadas), "n_ok": len(ok),
            "n_fallidas": len(fallidas),
            "archivos": regs[0].get("archivos"), "filas": regs[0].get("filas"), "gb_csv": regs[0].get("gb_csv"),
            "detalle": next((r.get("detalle") for r in regs if r.get("detalle")), None),
        }
        for campo in ("duracion_seg", "pico_ram_gb", "cpu_promedio_pct", "cpu_pico_pct",
                      "nucleos_efectivos", "ram_sistema_pico_pct", "swap_incremento_pico_gb",
                      "pico_memoria_privada_gb"):
            e = estadisticos([r.get(campo) for r in base])
            for k, v in e.items():
                fila[f"{campo}_{k}"] = v
        filas.append(fila)
    filas.sort(key=lambda f: (f["muestra_pct"], ORDEN.index(f["version"]) if f["version"] in ORDEN else 99))
    return filas


def buscar(filas: list[dict], version: str, pct: float) -> dict | None:
    return next((f for f in filas if f["version"] == version and f["muestra_pct"] == pct), None)


# ----------------------------------------------------------------------------
# Tablas
# ----------------------------------------------------------------------------
def celda_tiempo(f: dict | None) -> str:
    if f is None:
        return "-"
    if f["estado"] in ("OK", "PARCIAL"):
        txt = tiempo_legible(f["duracion_seg_media"])
        if f["duracion_seg_desv"] is not None:
            txt += f" ± {num(f['duracion_seg_desv'], 1)} s"
        if not UNA_EJECUCION:
            txt += f" (n={f['n_ok']})"
        if f["estado"] == "PARCIAL":
            txt += f"; {f['n_fallidas']} fallida(s)"
        return txt
    if f["estado"] == "OMITIDO":
        return "No ejecutado¹"
    if f.get("detalle") and f["detalle"].startswith("Más de"):
        return f"{ETIQUETA_ESTADO.get(f['estado'], f['estado'])} tras más de {num(f['duracion_seg_media'] / 3600, 1)} h"
    return (f"{ETIQUETA_ESTADO.get(f['estado'], f['estado'])} tras {tiempo_legible(f['duracion_seg_media'])}"
            + ("" if UNA_EJECUCION else f" (n={f['n_fallidas']})"))


def celda_ram(f: dict | None) -> str:
    if f is None or f["estado"] == "OMITIDO":
        return "-"
    if f["pico_ram_gb_media"] is None:
        return "No registrado"
    txt = f"{num(f['pico_ram_gb_media'], 2)}"
    if f["pico_ram_gb_desv"] is not None:
        txt += f" ± {num(f['pico_ram_gb_desv'], 2)}"
    if f["estado"] not in ("OK", "PARCIAL"):
        txt += " (hasta la falla)"
    return txt


def celda_cpu(f: dict | None) -> str:
    if f is None or f["estado"] == "OMITIDO" or f["cpu_promedio_pct_media"] is None:
        return "-"
    return f"{num(f['cpu_promedio_pct_media'], 0)} % / {num(f['nucleos_efectivos_media'], 1)}"


def aceleracion(filas: list[dict], version_pandas: str, pct: float) -> str:
    """Cuántas veces más rápido es Spark que la versión pandas indicada."""
    p, s = buscar(filas, version_pandas, pct), buscar(filas, "spark", pct)
    if not p or not s or s["estado"] not in ("OK", "PARCIAL"):
        return "-"
    if p["estado"] in ("OK", "PARCIAL"):
        return f"{num(p['duracion_seg_media'] / s['duracion_seg_media'], 2)} ×"
    if p["estado"] in ("MEMORY_ERROR", "TIMEOUT"):
        return "solo Spark termina"
    return "-"


def hay(filas: list[dict], campo: str) -> bool:
    return any(f.get(f"{campo}_media") is not None for f in filas)


def tablas_markdown(filas: list[dict], muestras: list[float], ram_total: float | None,
                    num_tablas: tuple[int, int, int]) -> str:
    t1, t2, t3 = num_tablas
    lineas = []
    if UNA_EJECUCION:
        lineas.append(f"**Tabla {t1}.** *Tiempo de ejecución por variante y muestra (una ejecución por "
                      "combinación, mediciones de la sección 5) y aceleración de Spark*\n")
    else:
        lineas.append(f"**Tabla {t1}.** *Tiempo de ejecución de extremo a extremo (media ± desviación "
                      "estándar de las repeticiones que terminaron) y aceleración de Spark*\n")
    lineas.append("| Muestra | Filas | CSV (GB) | pandas directo | pandas mitigado | PySpark | "
                  "Spark vs. directo | Spark vs. mitigado |")
    lineas.append("|---|---:|---:|---|---|---|---:|---:|")
    for pct in muestras:
        ref = next((f for f in filas if f["muestra_pct"] == pct), None)
        lineas.append(
            f"| {pct:g} % | {num(ref['filas'], 0) if ref else '-'} | {num(ref['gb_csv'], 2 if ref['gb_csv'] >= 0.1 else 3) if ref else '-'} | "
            f"{celda_tiempo(buscar(filas, 'pandas_directo', pct))} | "
            f"{celda_tiempo(buscar(filas, 'pandas_mitigado', pct))} | "
            f"{celda_tiempo(buscar(filas, 'spark', pct))} | "
            f"{aceleracion(filas, 'pandas_directo', pct)} | {aceleracion(filas, 'pandas_mitigado', pct)} |")
    nota = "\n"
    if any(f["estado"] == "OMITIDO" for f in filas):
        nota += ("¹ No se ejecutó porque la misma versión falló en todas las repeticiones con una "
                 "muestra menor o una repetición anterior superó el tiempo límite. ")
    nota += "La aceleración es el tiempo de pandas dividido entre el de Spark (mayor que 1: Spark es más rápido).\n"
    lineas.append(nota)

    con_sistema = hay(filas, "ram_sistema_pico_pct")
    if UNA_EJECUCION:
        lineas.append(f"**Tabla {t2}.** *Pico de memoria RAM en GB por variante y muestra (mediciones de la sección 5)*\n")
    else:
        lineas.append(f"**Tabla {t2}.** *Pico de memoria RAM del árbol de procesos, en GB (media ± desviación "
                      "estándar)" + (", y pico de RAM ocupada en todo el sistema*\n" if con_sistema else "*\n"))
    lineas.append("| Muestra | pandas directo | pandas mitigado | PySpark |" + (" RAM del sistema (pico) |" if con_sistema else ""))
    lineas.append("|---|---|---|---|" + ("---|" if con_sistema else ""))
    for pct in muestras:
        sistema = []
        for v in ORDEN:
            f = buscar(filas, v, pct)
            if f and f["ram_sistema_pico_pct_max"] is not None:
                sistema.append(f"{CORTOS[v]}: {num(f['ram_sistema_pico_pct_max'], 0)} %")
        lineas.append(f"| {pct:g} % | {celda_ram(buscar(filas, 'pandas_directo', pct))} | "
                      f"{celda_ram(buscar(filas, 'pandas_mitigado', pct))} | "
                      f"{celda_ram(buscar(filas, 'spark', pct))} |" + (f" {'; '.join(sistema) or '-'} |" if con_sistema else ""))
    if ram_total:
        lineas.append(f"\nRAM física del equipo: {num(ram_total, 2)} GB."
                      + (" «No registrado»: la ejecución falló antes de que se registrara el pico de memoria.\n"
                         if UNA_EJECUCION else " En Spark se suma el proceso Python y la JVM hija, que es la que "
                         "procesa los datos.\n"))

    if not hay(filas, "nucleos_efectivos"):
        return "\n".join(lineas) + "\n"
    lineas.append(f"**Tabla {t3}.** *Uso de CPU: promedio del equipo (100 % = todos los hilos) / "
                  "núcleos efectivos (tiempo de CPU ÷ tiempo real)*\n")
    lineas.append("| Muestra | pandas directo | pandas mitigado | PySpark |")
    lineas.append("|---|---|---|---|")
    for pct in muestras:
        lineas.append(f"| {pct:g} % | {celda_cpu(buscar(filas, 'pandas_directo', pct))} | "
                      f"{celda_cpu(buscar(filas, 'pandas_mitigado', pct))} | "
                      f"{celda_cpu(buscar(filas, 'spark', pct))} |")
    return "\n".join(lineas) + "\n"


# ----------------------------------------------------------------------------
# Figuras
# ----------------------------------------------------------------------------
def estilo_ejes(ax) -> None:
    ax.set_facecolor(SUPERFICIE)
    for lado in ("top", "right", "left"):
        ax.spines[lado].set_visible(False)
    ax.spines["bottom"].set_color(TINTA_2)
    ax.tick_params(colors=TINTA_2, labelsize=9, length=0)
    ax.grid(axis="y", color=REJILLA, linewidth=0.8)
    ax.set_axisbelow(True)


def leyenda(ax, extra: list | None = None, ubicacion: str = "upper left") -> None:
    manejadores = [Line2D([0], [0], color=COLORES[v], marker=MARCADORES[v], markersize=7, linewidth=2,
                          label=NOMBRES[v]) for v in ORDEN]
    manejadores += extra or []
    return ax.legend(handles=manejadores, loc="lower left", bbox_to_anchor=(0, 1.01), ncol=len(manejadores),
                     fontsize=8.2, frameon=False, labelcolor=TINTA, handlelength=2.2, columnspacing=1.2,
                     borderaxespad=0)


def figura_lineas(filas: list[dict], muestras: list[float], campo: str, titulo_y: str, archivo: Path,
                  log: bool = False, ram_total: float | None = None) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 4.0), dpi=200)
    fig.patch.set_facecolor(SUPERFICIE)
    estilo_ejes(ax)
    x_de = {pct: i for i, pct in enumerate(muestras)}
    hubo_falla = False
    for v in ORDEN:
        xs, ys, lo, hi = [], [], [], []
        for pct in muestras:
            f = buscar(filas, v, pct)
            if f and f["estado"] in ("OK", "PARCIAL") and f[f"{campo}_media"] is not None:
                xs.append(x_de[pct]); ys.append(f[f"{campo}_media"])
                lo.append(f[f"{campo}_media"] - f[f"{campo}_min"]); hi.append(f[f"{campo}_max"] - f[f"{campo}_media"])
        if xs:
            ax.errorbar(xs, ys, yerr=[lo, hi], color=COLORES[v], marker=MARCADORES[v], markersize=7,
                        linewidth=2, capsize=3, elinewidth=1.2, markeredgecolor=SUPERFICIE,
                        markeredgewidth=1.5, zorder=3)
        # Fallas: X del color de la serie en el valor medido hasta la falla, con etiqueta
        primera = True
        for pct in muestras:
            f = buscar(filas, v, pct)
            if f and f["estado"] in ("MEMORY_ERROR", "TIMEOUT", "ERROR") and f[f"{campo}_media"] is not None:
                hubo_falla = True
                y = f[f"{campo}_media"]
                ax.plot(x_de[pct], y, marker="X", markersize=11, color=COLORES[v],
                        markeredgecolor=SUPERFICIE, markeredgewidth=1.2, linestyle="none", zorder=4)
                if xs and primera:  # línea punteada desde el último punto exitoso hasta la primera falla
                    ax.plot([xs[-1], x_de[pct]], [ys[-1], y], color=COLORES[v], linewidth=1.5,
                            linestyle=(0, (3, 2)), zorder=2)
                primera = False
                ax.annotate(ETIQUETA_ESTADO[f["estado"]], (x_de[pct], y), textcoords="offset points",
                            xytext=(8, -3), fontsize=8, color=TINTA, zorder=5)
    if ram_total:
        ax.set_ylim(0, ram_total * 1.12)
        ax.axhline(ram_total, color=TINTA_2, linewidth=1.2, linestyle=(0, (5, 3)), zorder=1)
        ax.annotate(f"RAM física: {num(ram_total, 2)} GB", (len(muestras) - 1, ram_total),
                    textcoords="offset points", xytext=(0, 5), ha="right", fontsize=8, color=TINTA_2)
    if log:
        ax.set_yscale("log")
        ax.yaxis.set_major_locator(matplotlib.ticker.LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
        ax.yaxis.set_minor_locator(matplotlib.ticker.NullLocator())
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: num(v, 0 if v >= 1 else 1)))
    elif not ram_total:
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: num(v, 0 if v >= 10 else 1)))
    ax.set_xticks(range(len(muestras)), [f"{p:g} %" for p in muestras])
    ax.set_xlim(-0.3, len(muestras) - 0.7)
    ax.set_xlabel("Muestra de la tabla de hechos", color=TINTA_2, fontsize=9)
    ax.set_ylabel(titulo_y, color=TINTA_2, fontsize=9)
    extra = [Line2D([0], [0], color=TINTA_2, marker="X", markersize=9, linestyle="none",
                    label="Falla")] if hubo_falla else None
    leyenda(ax, extra)
    fig.tight_layout()
    fig.savefig(archivo, facecolor=SUPERFICIE)
    plt.close(fig)


def figura_cpu(filas: list[dict], muestras: list[float], hilos: int | None, archivo: Path) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 3.8), dpi=200)
    fig.patch.set_facecolor(SUPERFICIE)
    estilo_ejes(ax)
    ancho = 0.26
    for j, v in enumerate(ORDEN):
        for i, pct in enumerate(muestras):
            f = buscar(filas, v, pct)
            if not f or f["estado"] == "OMITIDO" or f["nucleos_efectivos_media"] is None:
                continue
            x = i + (j - 1) * ancho
            ok = f["estado"] in ("OK", "PARCIAL")
            ax.bar(x, f["nucleos_efectivos_media"], width=ancho - 0.03, color=COLORES[v],
                   edgecolor=SUPERFICIE, linewidth=1.5, hatch=None if ok else "///", alpha=1 if ok else 0.55,
                   zorder=3)
    if hilos:
        ax.axhline(hilos, color=TINTA_2, linewidth=1.2, linestyle=(0, (5, 3)))
        ax.annotate(f"Hilos lógicos del equipo: {hilos}", (len(muestras) - 0.55, hilos), textcoords="offset points",
                    xytext=(0, 4), ha="right", fontsize=8, color=TINTA_2)
        ax.set_ylim(0, hilos * 1.12)
    ax.set_xticks(range(len(muestras)), [f"{p:g} %" for p in muestras])
    ax.set_xlabel("Muestra de la tabla de hechos", color=TINTA_2, fontsize=9)
    ax.set_ylabel("Núcleos efectivos (tiempo de CPU ÷ tiempo real)", color=TINTA_2, fontsize=9)
    ax.yaxis.set_major_locator(matplotlib.ticker.MaxNLocator(integer=True))
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: num(v, 0)))
    ax.set_xlim(-0.5, len(muestras) - 0.5)
    manejadores = [matplotlib.patches.Patch(color=COLORES[v], label=NOMBRES[v]) for v in ORDEN]
    if any(f["estado"] in ("MEMORY_ERROR", "TIMEOUT", "ERROR") for f in filas):
        manejadores.append(matplotlib.patches.Patch(facecolor="white", edgecolor=TINTA_2, hatch="///",
                                                    label="Ejecución fallida"))
    ax.legend(handles=manejadores, loc="lower left", bbox_to_anchor=(0, 1.01), ncol=len(manejadores),
              fontsize=8.2, frameon=False, labelcolor=TINTA, borderaxespad=0)
    fig.tight_layout()
    fig.savefig(archivo, facecolor=SUPERFICIE)
    plt.close(fig)


# ----------------------------------------------------------------------------
# Resumen para el texto del informe
# ----------------------------------------------------------------------------
def resumen_para_informe(filas: list[dict], registros: list[dict], muestras: list[float], metadatos: dict) -> dict:
    def ok(v, p):
        f = buscar(filas, v, p)
        return f if f and f["estado"] in ("OK", "PARCIAL") else None

    res: dict = {"muestras": muestras, "equipo": metadatos.get("equipo", {}),
                 "fuente": metadatos.get("fuente", "medir.py"), "hay_cpu": hay(filas, "nucleos_efectivos"),
                 "parametros": metadatos.get("parametros", {})}
    res["repeticiones_min"] = min((f["n_ejecutadas"] for f in filas if f["n_ejecutadas"]), default=0)
    res["total_ejecuciones"] = sum(1 for r in registros if r["estado"] != "OMITIDO")
    res["horas_medicion"] = round(sum(r.get("duracion_seg") or 0 for r in registros) / 3600, 2)
    # Mayor muestra completada por cada versión y primera falla
    for v in ORDEN:
        completadas = [p for p in muestras if ok(v, p)]
        fallas = [p for p in muestras if (buscar(filas, v, p) or {}).get("estado") in ("MEMORY_ERROR", "TIMEOUT", "ERROR")]
        res[v] = {
            "max_muestra_ok": max(completadas) if completadas else None,
            "primera_falla": min(fallas) if fallas else None,
            "estado_primera_falla": buscar(filas, v, min(fallas))["estado"] if fallas else None,
            "por_muestra": {f"{p:g}": buscar(filas, v, p) for p in muestras},
        }
    # Punto de cruce: menor muestra en que Spark es más rápido que pandas directo
    cruce = None
    for p in muestras:
        s, d = ok("spark", p), buscar(filas, "pandas_directo", p)
        if s and d and (d["estado"] not in ("OK", "PARCIAL") or s["duracion_seg_media"] < d["duracion_seg_media"]):
            cruce = p
            break
    res["cruce_spark_vs_directo"] = cruce
    # Relaciones de crecimiento entre la menor y la mayor muestra completada
    for v in ORDEN:
        ps = [p for p in muestras if ok(v, p)]
        if len(ps) >= 2:
            a, b = ok(v, ps[0]), ok(v, ps[-1])
            res[v]["crecimiento_tiempo"] = b["duracion_seg_media"] / a["duracion_seg_media"]
            res[v]["crecimiento_datos"] = ps[-1] / ps[0]
            res[v]["crecimiento_ram"] = b["pico_ram_gb_media"] / a["pico_ram_gb_media"]
            res[v]["rango"] = [ps[0], ps[-1]]
    # Coeficiente de variación máximo entre repeticiones (estabilidad de la medición)
    cvs = [f["duracion_seg_desv"] / f["duracion_seg_media"] for f in filas
           if f["estado"] in ("OK", "PARCIAL") and f["duracion_seg_desv"] is not None and f["duracion_seg_media"]]
    res["cv_max_tiempo"] = max(cvs) if cvs else None
    res["cv_mediana_tiempo"] = st.median(cvs) if cvs else None
    return res


# ----------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="Tablas y figuras del benchmark")
    ap.add_argument("--entrada", type=Path, default=DIR_BENCH / "mediciones.jsonl")
    ap.add_argument("--salida", type=Path, default=None, help="Carpeta de salida (default: la de la entrada)")
    ap.add_argument("--tablas", type=int, nargs=3, default=[11, 12, 13],
                    help="Números de las tres tablas en el informe (default 11 12 13)")
    args = ap.parse_args()
    salida = args.salida or args.entrada.parent
    salida.mkdir(parents=True, exist_ok=True)

    registros = leer(args.entrada)
    if not registros:
        raise SystemExit(f"No hay mediciones en {args.entrada}. Ejecute primero benchmark/medir.py")
    ruta_meta = args.entrada.parent / "metadatos.json"
    metadatos = json.loads(ruta_meta.read_text(encoding="utf-8")) if ruta_meta.exists() else {}
    ram_total = metadatos.get("equipo", {}).get("ram_total_gb")
    hilos = metadatos.get("equipo", {}).get("hilos_logicos")

    global UNA_EJECUCION
    UNA_EJECUCION = metadatos.get("fuente") == "seccion5"
    filas = agregar(registros)
    muestras = sorted({f["muestra_pct"] for f in filas})

    # CSV
    with open(salida / "resumen_por_muestra.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)
    campos = sorted({k for r in registros for k in r.keys()})
    with open(salida / "mediciones_detalle.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        w.writerows(registros)

    (salida / "tablas.md").write_text(tablas_markdown(filas, muestras, ram_total, tuple(args.tablas)),
                                      encoding="utf-8")
    figura_lineas(filas, muestras, "duracion_seg", "Tiempo de ejecución (s, escala logarítmica)",
                  salida / "fig_tiempo.png", log=True)
    figura_lineas(filas, muestras, "pico_ram_gb", "Pico de memoria RAM (GB)",
                  salida / "fig_memoria.png", ram_total=ram_total)
    if hay(filas, "nucleos_efectivos"):
        figura_cpu(filas, muestras, hilos, salida / "fig_cpu.png")
    elif (salida / "fig_cpu.png").exists():
        (salida / "fig_cpu.png").unlink()

    resumen = resumen_para_informe(filas, registros, muestras, metadatos)
    (salida / "resumen.json").write_text(json.dumps(resumen, indent=2, ensure_ascii=False, default=str),
                                         encoding="utf-8")

    print(f"{len(registros)} ejecuciones leídas -> {len(filas)} combinaciones versión/muestra")
    print((salida / "tablas.md").read_text(encoding="utf-8"))
    print(f"Archivos en {salida}: tablas.md, resumen_por_muestra.csv, mediciones_detalle.csv, "
          "resumen.json, fig_tiempo.png, fig_memoria.png, fig_cpu.png")


if __name__ == "__main__":
    main()
