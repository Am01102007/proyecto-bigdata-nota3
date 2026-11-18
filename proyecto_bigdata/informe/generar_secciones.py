"""
informe/generar_secciones.py
============================
Genera las secciones 6 (Análisis comparativo), 7 (Conclusiones) y 8 (Referencias) del
informe a partir de los resultados reales del benchmark. Así las cifras del texto, de las
tablas y de las figuras salen siempre de la misma fuente y no pueden contradecirse.

Entradas (las producen los scripts de benchmark/):
    evidencias/benchmark/resumen.json, tablas.md, fig_tiempo.png, fig_memoria.png, fig_cpu.png
    evidencias/benchmark/complejidad_codigo.json y .md

Si todavía no hay mediciones, las partes que dependen de ellas se marcan como
PENDIENTE para que se note en el borrador.

Uso:
    python benchmark/medir.py && python benchmark/graficos.py && python benchmark/complejidad_codigo.py
    python informe/generar_secciones.py         # -> informe/secciones_6_7_8.md
"""

from __future__ import annotations

import json
import re
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
BENCH = RAIZ / "evidencias" / "benchmark"
SALIDA = RAIZ / "informe" / "secciones_6_7_8.md"

# Numeración de tablas y figuras (continúa la de las secciones 1-5 del informe)
T_TIEMPO, T_RAM, T_CPU, T_CODIGO = 11, 12, 13, 14
F_TIEMPO, F_RAM, F_CPU = 8, 9, 10

NOMBRE = {"pandas_directo": "pandas directo", "pandas_mitigado": "pandas mitigado", "spark": "Spark"}
FALLA = {"MEMORY_ERROR": "un error de memoria (MemoryError)", "TIMEOUT": "superar el tiempo límite",
         "ERROR": "un error de ejecución"}


def num(x, dec=1):
    if x is None:
        return "–"
    return f"{x:,.{dec}f}".replace(",", "§").replace(".", ",").replace("§", ".")


def t(seg):
    if seg is None:
        return "–"
    if seg < 120:
        return f"{num(seg, 1)} s"
    if seg % 3600 == 0:
        return f"{int(seg // 3600)} h"
    if seg < 7200:
        return f"{num(seg / 60, 1)} min"
    return f"{num(seg / 3600, 2)} h"


def pend(texto):
    return f"**[PENDIENTE: {texto}]**"


# ----------------------------------------------------------------------------
# Texto que depende de las mediciones
# ----------------------------------------------------------------------------
class Datos:
    def __init__(self, r: dict):
        self.r = r
        self.muestras = [float(m) for m in r["muestras"]]
        self.eq = r.get("equipo", {})

    def f(self, v, p):
        return (self.r.get(v, {}).get("por_muestra", {}) or {}).get(f"{p:g}")

    def ok(self, v, p):
        f = self.f(v, p)
        return f if f and f["estado"] in ("OK", "PARCIAL") else None

    def media(self, v, p, campo="duracion_seg"):
        f = self.f(v, p)
        return f.get(f"{campo}_media") if f else None

    def velocidad(self, v, p):
        """pandas / Spark (>1: Spark más rápido)."""
        a, s = self.ok(v, p), self.ok("spark", p)
        return a["duracion_seg_media"] / s["duracion_seg_media"] if a and s else None


def texto_protocolo_seccion5(d: Datos) -> str:
    eq = d.eq
    procesador = re.sub(r"\(R\)|\(TM\)", "", eq.get("procesador", "")).replace("  ", " ").strip()
    return f"""## 6.1 Datos y método de comparación

Las cifras de esta sección son las mediciones de la sección 5, tomadas en el equipo de pruebas ({procesador}, {eq.get('nucleos_fisicos', '?')} núcleos y {eq.get('hilos_logicos', '?')} hilos, {num(eq.get('ram_total_gb'), 2)} GB de RAM, {eq.get('sistema_operativo', 'Windows')}) con las mismas cuatro muestras para las tres variantes:

- **pandas directo:** `version_pandas/main.py`, que carga toda la muestra en memoria.
- **pandas mitigado:** la misma versión con `--mitigado`, que aplica las tres mitigaciones del enunciado (lectura por *chunks*, tipos optimizados y selección de columnas).
- **PySpark:** `version_spark/main.py` en modo `local[*]`, con la configuración de la Tabla 8.

En cada ejecución se registró el tiempo total y el pico de memoria física con psutil (Rodola, 2024). Aquí se reorganizan esos datos para compararlos: se calcula cuántas veces más rápido es Spark que cada variante de pandas, se grafica la escalabilidad y se interpreta cada resultado. Las figuras se generaron con matplotlib (Hunter, 2007).

Hay que tener en cuenta un límite: cada combinación se midió **una sola vez**, mientras que el enunciado pide al menos tres repeticiones. Por eso el repositorio incluye `benchmark/medir.py`, que repite cada medición tres veces y lanza cada repetición como un proceso nuevo. El script mide además la memoria y la CPU de todo el árbol de procesos, incluida la JVM de Spark, intercala las versiones para no favorecer a ninguna con la caché de disco y regenera esta sección con `benchmark/graficos.py` e `informe/generar_secciones.py`. Salvo con la muestra del 1 %, las diferencias observadas entre pandas y Spark son de varias veces o directamente entre terminar y fallar, mucho mayores que la variación habitual entre repeticiones de un mismo proceso, así que las conclusiones cualitativas no dependen de ese límite.
"""


def texto_protocolo(d: Datos | None, params: dict) -> str:
    if d and d.r.get("fuente") == "seccion5":
        return texto_protocolo_seccion5(d)
    if d:
        eq = d.eq
        equipo = (f"{eq.get('procesador', 'procesador no registrado')} "
                  f"({eq.get('nucleos_fisicos', '?')} núcleos, {eq.get('hilos_logicos', '?')} hilos), "
                  f"{num(eq.get('ram_total_gb'), 2)} GB de RAM y {eq.get('sistema_operativo', 'Windows')}")
        reps = params.get("repeticiones", 3)
        limite = t(params.get("timeout_seg", 7200))
        horas = num(d.r.get("horas_medicion"), 1)
        total = d.r.get("total_ejecuciones")
    else:
        equipo, reps, limite, horas, total = pend("equipo de pruebas (evidencias/benchmark/metadatos.json)"), 3, "2 h", "–", "–"
    return f"""## 6.1 Protocolo de medición

Las mediciones se tomaron con el script `benchmark/medir.py` en el mismo equipo descrito en la sección 3 ({equipo}). Se compararon tres variantes que ejecutan el mismo flujo (lectura, limpieza, joins, cuatro consultas y escritura en Parquet) sobre las mismas muestras de 1 %, 10 %, 50 % y 100 % de los archivos:

- **pandas directo:** `version_pandas/main.py`, que carga toda la muestra en memoria.
- **pandas mitigado:** la misma versión con `--mitigado`, que aplica las tres mitigaciones del enunciado (lectura por *chunks*, tipos optimizados y selección de columnas).
- **PySpark:** `version_spark/main.py` en modo `local[*]`, con la configuración de la Tabla 8.

Para que la comparación fuera justa se tomaron cinco decisiones de diseño:

1. **Cada repetición es un proceso nuevo.** pandas parte de un intérprete limpio y Spark de una JVM limpia. Además, un `MemoryError` no detiene el resto del experimento.
2. **Se mide el árbol de procesos completo.** En Spark el trabajo no lo hace el proceso de Python sino la JVM que este lanza como proceso hijo. El script suma la memoria (RSS, el *working set* que muestra el Administrador de tareas) y el tiempo de CPU de todos los procesos del árbol, muestreados con psutil (Rodola, 2024) cada {num(params.get('intervalo_monitor_seg', 0.25), 2)} s. Si solo se midiera el proceso de Python, Spark aparentaría usar unos 100 MB.
3. **El tiempo es de extremo a extremo.** Incluye el arranque del intérprete y de la JVM, porque es el tiempo que espera el usuario.
4. **Las versiones se intercalan.** En cada repetición se ejecutan pandas directo, pandas mitigado y Spark, una tras otra. Así la caché de disco del sistema operativo y la temperatura del procesador no favorecen siempre a la misma versión.
5. **Repeticiones y límites.** Cada combinación se repitió {reps} veces, con un tiempo límite de {limite} por ejecución. Si una versión falló en todas las repeticiones de una muestra, sus muestras mayores se registraron como *no ejecutadas*, porque no podrían terminar con más datos.

En total se registraron {total} ejecuciones, que sumaron {horas} horas de cómputo. La CPU se reporta de dos formas: el uso promedio del equipo (100 % significa todos los hilos ocupados) y los **núcleos efectivos**, que equivalen al tiempo de CPU dividido entre el tiempo real. Un proceso de un solo hilo da como máximo 1, y Spark puede acercarse al número de hilos del equipo. Las tablas y figuras se generan con `benchmark/graficos.py` y matplotlib (Hunter, 2007). Los registros de cada ejecución están en `evidencias/benchmark/logs/` y los datos crudos en `evidencias/benchmark/mediciones.jsonl`.
"""


def texto_tiempo(d: Datos | None, tablas: str | None) -> str:
    cuerpo = f"## 6.2 Tiempo de ejecución y escalabilidad\n\n"
    if not d:
        return cuerpo + pend(f"ejecutar benchmark/medir.py y benchmark/graficos.py para generar la Tabla {T_TIEMPO} y la Figura {F_TIEMPO}") + "\n"
    cuerpo += (f"La Tabla {T_TIEMPO} resume el tiempo de las tres variantes y la Figura {F_TIEMPO} lo muestra "
               "en escala logarítmica, en la que una recta de pendiente constante indica un crecimiento "
               "proporcional al volumen.\n\n")
    cuerpo += bloque_tabla(tablas, 0) + "\n"
    detalle_fig = ("Cada punto es una ejecución (sección 5); las X marcan el tiempo hasta la falla."
                   if d.r.get("fuente") == "seccion5" else
                   "Los puntos son la media y las barras van del mínimo al máximo de las repeticiones; las X marcan ejecuciones que fallaron.")
    cuerpo += f"![](../evidencias/benchmark/fig_tiempo.png)\n\n**Figura {F_TIEMPO}.** *Tiempo de ejecución frente al tamaño de la muestra (escala logarítmica). {detalle_fig}*\n\n"

    p1, pmax = d.muestras[0], d.muestras[-1]
    parrafos = []
    # Muestra pequeña: sobrecarga de Spark
    v1, s1 = d.ok("pandas_directo", p1), d.ok("spark", p1)
    if v1 and s1:
        if s1["duracion_seg_media"] > v1["duracion_seg_media"]:
            parrafos.append(
                f"**Con pocos datos gana pandas.** Con la muestra del {p1:g} % pandas directo terminó en "
                f"{t(v1['duracion_seg_media'])} y Spark en {t(s1['duracion_seg_media'])}, es decir, Spark fue "
                f"{num(s1['duracion_seg_media'] / v1['duracion_seg_media'], 1)} veces más lento. La diferencia no se "
                "debe al procesamiento sino al costo fijo de Spark: arrancar la JVM, crear la SparkSession, planificar "
                "el DAG y coordinar las tareas. Con un volumen tan pequeño ese costo fijo pesa más que el trabajo útil.")
        else:
            parrafos.append(
                f"Incluso con la muestra del {p1:g} % Spark ({t(s1['duracion_seg_media'])}) fue más rápido que pandas "
                f"directo ({t(v1['duracion_seg_media'])}), a pesar del costo fijo de arrancar la JVM.")
    cruce = d.r.get("cruce_spark_vs_directo")
    if cruce is not None and cruce != p1:
        a = d.ok("pandas_directo", cruce)
        if a:
            parrafos.append(
                f"**El punto de cruce está en la muestra del {cruce:g} %.** A partir de ahí Spark supera a pandas "
                f"directo ({t(d.media('spark', cruce))} frente a {t(a['duracion_seg_media'])}, "
                f"{num(d.velocidad('pandas_directo', cruce), 1)} veces más rápido), porque el trabajo útil ya es mayor "
                "que el costo fijo y Spark lo reparte entre todos los núcleos.")
        else:
            parrafos.append(f"**A partir de la muestra del {cruce:g} % solo Spark termina el procesamiento.**")
    # Falla de pandas directo
    pd_ = d.r.get("pandas_directo", {})
    if pd_.get("primera_falla") is not None:
        pf = pd_["primera_falla"]
        f = d.f("pandas_directo", pf)
        parrafos.append(
            f"**pandas directo deja de funcionar con el {pf:g} %.** Completó hasta la muestra del "
            f"{num(pd_.get('max_muestra_ok'), 0) if pd_.get('max_muestra_ok') else '–'} % y con el {pf:g} % falló por "
            f"{FALLA.get(pd_['estado_primera_falla'], 'un error')} después de {t(f['duracion_seg_media'])}"
            + ("" if d.r.get("fuente") == "seccion5" else " en promedio")
            + ("" if d.r.get("fuente") == "seccion5" else f" ({f['n_fallidas']} de {f['n_ejecutadas']} repeticiones)")
            + ". Es el comportamiento documentado en la sección 5.")
    elif pd_.get("max_muestra_ok") == pmax:
        parrafos.append(f"pandas directo llegó a completar la muestra del {pmax:g} %, en "
                        f"{t(d.media('pandas_directo', pmax))}; ver la discusión de la sección 6.6.")
    # Mitigado
    pm = d.r.get("pandas_mitigado", {})
    if pm.get("max_muestra_ok") is not None:
        titulo = ("**Las mitigaciones extienden el alcance de pandas, pero no lo vuelven viable.**"
                  if pd_.get("primera_falla") is not None else "**pandas mitigado.**")
        frase = (f"{titulo} pandas mitigado "
                 f"completó hasta la muestra del {pm['max_muestra_ok']:g} %, en "
                 f"{t(d.media('pandas_mitigado', pm['max_muestra_ok']))}")
        sm = d.velocidad("pandas_mitigado", pm["max_muestra_ok"])
        if sm and sm >= 1:
            frase += f", {num(sm, 1)} veces lo que tardó Spark con la misma muestra"
        elif sm:
            frase += f", menos que Spark con la misma muestra ({t(d.media('spark', pm['max_muestra_ok']))})"
        if pm.get("primera_falla") is not None:
            pf = pm["primera_falla"]
            ff = d.f("pandas_mitigado", pf)
            cuando = (f"más de {num(ff['duracion_seg_media'] / 3600, 1)} h" if (ff.get("detalle") or "").startswith("Más de")
                      else t(ff["duracion_seg_media"]))
            frase += f", y con el {pf:g} % falló por {FALLA.get(pm['estado_primera_falla'], 'un error')} tras {cuando}"
        frase += (". Además, aunque termine, su resultado no es exacto: la deduplicación solo ve los duplicados "
                  "dentro de cada fragmento, y el conteo de clientes únicos exige acumular conjuntos de "
                  "identificadores que crecen con los datos (sección 5.4). Por eso su tiempo no es comparable "
                  "en igualdad de condiciones con el de Spark, que sí entrega el resultado exacto.")
        parrafos.append(frase)
    # Spark al 100 %
    sx = d.ok("spark", pmax)
    if sx:
        frase = (f"**Spark completó todas las muestras.** Con el {pmax:g} % ({num(sx['filas'], 0)} filas, "
                 f"{num(sx['gb_csv'], 2)} GB de CSV) tardó {t(sx['duracion_seg_media'])}")
        if sx.get("duracion_seg_desv") is not None:
            frase += f" con una desviación estándar de {num(sx['duracion_seg_desv'], 1)} s"
        frase += "."
        sp = d.r.get("spark", {})
        if sp.get("crecimiento_tiempo"):
            frase += (f" Entre la muestra del {sp['rango'][0]:g} % y la del {sp['rango'][1]:g} % los datos se "
                      f"multiplicaron por {num(sp['crecimiento_datos'], 0)} y el tiempo por "
                      f"{num(sp['crecimiento_tiempo'], 1)}")
            if sp["crecimiento_tiempo"] < sp["crecimiento_datos"]:
                frase += (": el crecimiento es menor que proporcional porque el costo fijo de arranque se reparte "
                          "entre más datos y porque, con más archivos, todos los núcleos tienen trabajo.")
            else:
                frase += (": el crecimiento es proporcional o algo mayor, lo que se explica por los *shuffles* "
                          "(deduplicación y join con clientes), que escriben datos intermedios en disco cuando no "
                          "caben en la memoria asignada.")
        parrafos.append(frase)
    elif d.r.get("spark", {}).get("max_muestra_ok"):
        parrafos.append(pend(f"Spark no completó la muestra del {pmax:g} %; revisar el log antes de entregar"))
    cv = d.r.get("cv_mediana_tiempo")
    if cv is not None:
        parrafos.append(
            f"Las repeticiones fueron estables: la mediana del coeficiente de variación del tiempo fue "
            f"{num(cv * 100, 1)} % (máximo {num(d.r['cv_max_tiempo'] * 100, 1)} %), de modo que las diferencias "
            "entre versiones son mucho mayores que la variabilidad de la medición.")
    return cuerpo + "\n\n".join(parrafos) + "\n"


def memoria_spark(d: Datos) -> str:
    """spark.driver.memory efectiva: la de --args-spark o la regla de sesion.py (50 % de la RAM)."""
    partes = ((d.r.get("parametros") or {}).get("args_spark") or "").split()
    if "--memoria-driver" in partes and partes.index("--memoria-driver") + 1 < len(partes):
        return f"{partes[partes.index('--memoria-driver') + 1]} en estas mediciones"
    ram = d.eq.get("ram_total_gb")
    if ram:
        return f"{round(min(24, max(2, ram * 0.5)))} GB en este equipo, la mitad de la RAM según `sesion.py`"
    return "la mitad de la RAM según `sesion.py`"


def texto_siguiente(d: Datos, pct: float, factor: float, ram: float | None) -> str:
    """Estimación de la memoria que necesitaría pandas directo con la muestra siguiente."""
    mayores = [p for p in d.muestras if p > pct]
    if not mayores or not ram:
        return ""
    sig = d.f("pandas_directo", mayores[0]) or d.f("spark", mayores[0])
    if not sig or not sig.get("gb_csv"):
        return ""
    necesaria = factor * sig["gb_csv"]
    if necesaria > ram:
        return (f" Con ese mismo factor, la muestra del {mayores[0]:g} % ({num(sig['gb_csv'], 2)} GB de CSV) "
                f"necesitaría unos {num(necesaria, 1)} GB, más que los {num(ram, 2)} GB de RAM del equipo.")
    return (f" Con ese mismo factor, la muestra del {mayores[0]:g} % necesitaría unos {num(necesaria, 1)} GB, "
            f"el {num(necesaria / ram * 100, 0)} % de la RAM del equipo.")


def texto_memoria(d: Datos | None, tablas: str | None) -> str:
    cuerpo = "## 6.3 Consumo de memoria\n\n"
    if not d:
        return cuerpo + pend(f"Tabla {T_RAM} y Figura {F_RAM} (se generan con benchmark/graficos.py)") + "\n"
    ram = d.eq.get("ram_total_gb")
    cuerpo += (f"La Tabla {T_RAM} y la Figura {F_RAM} muestran el pico de memoria física de cada variante. "
               "La línea discontinua de la figura marca la RAM instalada.\n\n")
    cuerpo += bloque_tabla(tablas, 1) + "\n"
    detalle_fig = ("Cada punto es una ejecución (sección 5); no se grafican las ejecuciones que fallaron sin registrar memoria."
                   if d.r.get("fuente") == "seccion5" else
                   "Memoria de todo el árbol de procesos (en Spark, Python y la JVM). Las X indican la memoria alcanzada antes de una falla.")
    cuerpo += f"![](../evidencias/benchmark/fig_memoria.png)\n\n**Figura {F_RAM}.** *Pico de memoria RAM frente al tamaño de la muestra. {detalle_fig}*\n\n"
    parrafos = []
    pd_ = d.r.get("pandas_directo", {})
    pok = pd_.get("max_muestra_ok")
    if pok:
        f = d.ok("pandas_directo", pok)
        parrafos.append(
            f"**En pandas la memoria crece con los datos.** pandas directo necesitó {num(f['pico_ram_gb_media'], 2)} GB "
            f"para la muestra del {pok:g} %, cuyo CSV ocupa {num(f['gb_csv'], 2)} GB: un factor de "
            f"{num(f['pico_ram_gb_media'] / f['gb_csv'], 1)} entre memoria y disco"
            + (", muy cercano al 3,63 que la Tabla 3 midió para la sola lectura del CSV." if f['pico_ram_gb_media'] / f['gb_csv'] < 4.5
               else ". La Tabla 3 midió 3,63 solo para leer el CSV; el pipeline completo suma el intérprete, las tres "
                    "dimensiones (clientes tiene 5 millones de filas) y las copias intermedias que crean la limpieza, "
                    "los joins y las agregaciones.")
            + texto_siguiente(d, pok, f['pico_ram_gb_media'] / f['gb_csv'], ram))
    if pd_.get("primera_falla") is not None:
        f = d.f("pandas_directo", pd_["primera_falla"])
        if f and f.get("pico_ram_gb_media") is not None:
            sistema = f.get("ram_sistema_pico_pct_max")
            parrafos.append(
                f"Antes de fallar con el {pd_['primera_falla']:g} %, el proceso de pandas llegó a "
                f"{num(f['pico_ram_gb_media'], 2)} GB de memoria física"
                + (f" y el sistema completo al {num(sistema, 0)} % de la RAM" if sistema else "")
                + ". La cifra de memoria física es menor que lo que pandas intentó reservar, porque Windows envía "
                  "a disco (archivo de paginación) lo que no cabe; por eso el equipo se vuelve lento e inestable, como "
                  "muestran las capturas de la sección 5.3.")
    pm = d.r.get("pandas_mitigado", {})
    if pm.get("max_muestra_ok"):
        f = d.ok("pandas_mitigado", pm["max_muestra_ok"])
        parrafos.append(
            f"La versión mitigada consume menos ({num(f['pico_ram_gb_media'], 2)} GB con el {pm['max_muestra_ok']:g} %), "
            "pero su memoria también aumenta con el volumen, porque los conjuntos de clientes únicos y las "
            "agregaciones parciales se acumulan durante todo el recorrido.")
    s = [(p, d.ok("spark", p)) for p in d.muestras if d.ok("spark", p)]
    if s:
        picos = [x["pico_ram_gb_media"] for _, x in s]
        parrafos.append(
            f"**En Spark la memoria está acotada.** Su pico estuvo entre {num(min(picos), 2)} y {num(max(picos), 2)} GB "
            f"en todas las muestras: mientras los datos crecieron {num(s[-1][0] / s[0][0], 0)} veces, la memoria creció "
            f"{num(max(picos) / min(picos), 1)} veces. El límite lo "
            f"fija `spark.driver.memory` ({memoria_spark(d)}), que es el tamaño máximo del *heap* de la JVM; "
            "a eso se suman la memoria propia de la JVM y la del proceso de Python. Spark procesa una partición por "
            "núcleo, guarda en disco (*spill*) lo que no cabe en el *heap* y libera cada partición al terminarla."
            + (f" Con el {s[-1][0]:g} % el sistema completo llegó como máximo al "
               f"{num(s[-1][1].get('ram_sistema_pico_pct_max'), 0)} % de la RAM, sin comprometer la estabilidad del equipo."
               if s[-1][1].get('ram_sistema_pico_pct_max') is not None else
               f" Con el {s[-1][0]:g} %, el pico de {num(picos[-1], 2)} GB dejó margen para el sistema operativo"
               + (f", a diferencia de pandas directo, que con el {pd_['primera_falla']:g} % ya había agotado la memoria."
                  if pd_.get("primera_falla") is not None else ".")))
    return cuerpo + "\n\n".join(parrafos) + "\n"


def paralelismo_bajo(d: Datos, nucleos: list) -> str:
    p1 = d.muestras[0]
    n1 = d.media("spark", p1, "nucleos_efectivos") if d.ok("spark", p1) else None
    if n1 is not None and nucleos and n1 < 0.8 * max(nucleos):
        return (f" El paralelismo es menor con la muestra del {p1:g} % ({num(n1, 1)} núcleos), que tiene pocas "
                "particiones y en la que pesa más el arranque de la JVM, una fase de un solo hilo.")
    return ""


def texto_cpu(d: Datos | None, tablas: str | None) -> str:
    cuerpo = "## 6.4 Uso de CPU y paralelismo\n\n"
    if not d:
        return cuerpo + pend(f"Tabla {T_CPU} y Figura {F_CPU} (se generan con benchmark/graficos.py)") + "\n"
    hilos = d.eq.get("hilos_logicos")
    if not d.r.get("hay_cpu", True):
        return cuerpo + (
            "Las ejecuciones de la sección 5 no registraron el tiempo de CPU de Spark, de modo que esta parte se "
            "basa en lo observado en pandas y en el modelo de ejecución de Spark. La sección 5.4 documenta que "
            f"pandas ocupó un solo hilo de ejecución, el 8,3 % de la CPU de un procesador de {hilos} hilos: "
            "las operaciones de pandas corren en un único hilo del intérprete de Python y no hay un planificador "
            "que reparta el trabajo. Spark, en cambio, divide cada archivo en particiones de hasta 128 MB y en "
            f"modo `local[*]` ejecuta una tarea por hilo, es decir, hasta {hilos} tareas a la vez en este equipo.\n\n"
            "Los tiempos de la Tabla 11 son coherentes con esa diferencia: con el 10 % de los datos Spark fue "
            f"{num(d.velocidad('pandas_directo', 10.0), 1)} veces más rápido que pandas directo, una ventaja que no "
            "se explica por procesar cada fila más rápido, sino por procesar muchas a la vez. `benchmark/medir.py` "
            "registra los núcleos efectivos de cada variante (tiempo de CPU dividido entre tiempo real) para "
            "cuantificar este punto.\n")
    cuerpo += (f"La Tabla {T_CPU} y la Figura {F_CPU} muestran cuántos núcleos aprovechó cada variante.\n\n")
    cuerpo += bloque_tabla(tablas, 2) + "\n"
    cuerpo += f"![](../evidencias/benchmark/fig_cpu.png)\n\n**Figura {F_CPU}.** *Núcleos efectivos (tiempo de CPU dividido entre tiempo real) por variante y muestra. La línea discontinua marca los hilos lógicos del equipo; las barras rayadas corresponden a ejecuciones fallidas.*\n\n"
    nucleos = {v: [d.media(v, p, "nucleos_efectivos") for p in d.muestras if d.ok(v, p)] for v in NOMBRE}
    parrafos = []
    pandas_max = max([x for v in ("pandas_directo", "pandas_mitigado") for x in nucleos[v] if x], default=None)
    if pandas_max is not None:
        parrafos.append(
            f"**pandas usa un solo núcleo.** En ninguna muestra superó {num(pandas_max, 2)} núcleos efectivos, es "
            f"decir, dejó sin usar el {num((1 - pandas_max / hilos) * 100, 0) if hilos else '–'} % de la capacidad de los {hilos} hilos del procesador. La causa es estructural: las "
            "operaciones de pandas se ejecutan en un solo hilo del intérprete de Python y no hay un planificador que "
            "reparta el trabajo.")
    if nucleos["spark"]:
        mx = max(nucleos["spark"])
        parrafos.append(
            f"**Spark reparte el trabajo entre todos los núcleos.** Llegó a {num(mx, 1)} núcleos efectivos "
            f"({num(mx / hilos * 100 if hilos else 0, 0)} % de los hilos disponibles). Cada partición de unos 128 MB "
            "es una tarea independiente, y en modo `local[*]` se ejecutan tantas tareas a la vez como hilos tiene el "
            "equipo." + paralelismo_bajo(d, nucleos["spark"]) + " Esta es la principal razón de la diferencia de tiempos con volúmenes grandes: no es "
            "que Spark procese cada fila más rápido, sino que procesa muchas a la vez.")
    return cuerpo + "\n\n".join(parrafos) + "\n"


def texto_codigo(comp: dict | None, tabla_md: str | None) -> str:
    cuerpo = "## 6.5 Complejidad y legibilidad del código\n\n"
    if not comp:
        return cuerpo + pend("ejecutar benchmark/complejidad_codigo.py") + "\n"
    tp, ts = comp["totales"]["pandas"], comp["totales"]["spark"]
    etapa = {e["etapa"]: e for e in comp["etapas"]}
    mit = etapa["Mitigación por chunks"]["pandas"]
    joins_p, joins_s = etapa["Joins"]["pandas"], etapa["Joins"]["spark"]
    cuerpo += (
        f"La Tabla {T_CODIGO} compara las dos versiones con métricas objetivas, medidas por etapa con "
        "`benchmark/complejidad_codigo.py`. La complejidad ciclomática (McCabe, 1976) cuenta los caminos "
        "independientes de una función: cuanto más alta, más difícil es leerla y probarla.\n\n")
    cuerpo += tabla_md.strip() + "\n\n"
    cuerpo += (
        f"El tamaño total es parecido ({tp['loc']} líneas efectivas en pandas frente a {ts['loc']} en Spark), pero "
        f"la complejidad está repartida de forma muy distinta. En Spark la complejidad media por función es "
        f"{num(ts['cc_media_por_funcion'], 2)} y ninguna función supera {ts['cc_max']}; en pandas la media es "
        f"{num(tp['cc_media_por_funcion'], 2)} y el máximo llega a {tp['cc_max']}, en "
        f"`{mit['funcion_cc_max']}` ({mit['loc']} líneas). Esa función concentra el costo de las mitigaciones: "
        "tiene que repetir a mano lo que Spark hace solo (recorrer fragmentos, acumular resultados parciales de "
        "cada consulta, combinarlos al final y guardar conjuntos de identificadores para el conteo distinto).\n\n"
        "Tres diferencias explican el resultado:\n\n"
        "- **Spark es declarativo.** El código describe *qué* se quiere (filtros, joins, agrupaciones) y Catalyst "
        "decide *cómo* ejecutarlo: el orden de las operaciones, qué columnas leer, el tipo de join y el número de "
        "particiones. En pandas cada una de esas decisiones está escrita en el código o simplemente no se toma.\n"
        "- **El particionamiento es transparente.** En pandas, pasar de procesar todo en memoria a procesar por "
        "fragmentos obligó a escribir un segundo pipeline (`mitigaciones.py`) con lógica distinta para cada "
        "consulta. En Spark el mismo código sirve para 1 GB o para 100 GB, en un portátil o en un clúster; solo "
        "cambia la configuración de la sesión.\n"
        f"- **Las operaciones globales son una sola llamada.** La deduplicación global es `dropDuplicates`, el "
        "conteo exacto de clientes únicos es `countDistinct` y el top 10 por departamento es una función de ventana "
        f"con `rank()`. Los joins ocupan {joins_s['loc']} líneas en Spark frente a {joins_p['loc']} en pandas.\n\n"
        "El fragmento siguiente muestra el contraste en la consulta 4. En pandas mitigado los clientes únicos "
        "se acumulan en conjuntos de Python que crecen con los datos:\n\n"
        "```python\n"
        "# pandas mitigado (mitigaciones.py): estado que hay que mantener a mano entre fragmentos\n"
        "for (seg, mes), grupo in df_validos.groupby([\"segmento\", \"anio_mes\"])[\"cliente_id\"]:\n"
        "    sets_clientes_unicos.setdefault((str(seg), str(mes)), set()).update(grupo.values)\n"
        "...\n"
        "conteos = [{\"segmento\": s, \"anio_mes\": m, \"clientes_unicos\": len(st)}\n"
        "           for (s, m), st in sets_clientes_unicos.items()]\n"
        "```\n\n"
        "En Spark es una agregación más, y el motor se encarga del reparto por clave y del *spill* a disco:\n\n"
        "```python\n"
        "# PySpark (agregaciones.py)\n"
        ".groupBy(\"segmento\", \"anio_mes\")\n"
        ".agg(F.round(F.avg(\"venta_neta\"), 2).alias(\"ticket_promedio\"),\n"
        "     F.countDistinct(\"cliente_id\").alias(\"clientes_unicos\"), ...)\n"
        "```\n\n"
        f"Spark también tiene costos de legibilidad. Exige una etapa que pandas no necesita, la configuración de la "
        f"sesión (`sesion.py`, {etapa['Configuración del motor']['spark']['loc']} líneas), y un orquestador más "
        "largo, porque por la evaluación perezosa hay que provocar acciones explícitas (`count`, `write`) para medir "
        "cada etapa. Depurar también es más difícil: un error en una transformación aparece en la acción que la "
        "ejecuta, a veces varias líneas después, y la traza mezcla Python y Java. Para un analista que ya conoce "
        "pandas, la sintaxis de Spark es familiar (DataFrames, `groupBy`, `join`) y además admite SQL, como en la "
        "consulta 2, que resuelve en una sola pasada lo que en pandas requiere tres agrupaciones y dos `merge`.\n")
    return cuerpo


def texto_discusion(d: Datos | None) -> str:
    pequenos = "- **pandas sigue siendo la mejor opción con datos pequeños.** Con la muestra del 1 % fue más rápido y más simple."
    if d:
        p1 = d.muestras[0]
        a, s = d.ok("pandas_directo", p1), d.ok("spark", p1)
        if a and s and a["duracion_seg_media"] >= s["duracion_seg_media"]:
            pequenos = ("- **pandas sigue siendo una buena opción con datos pequeños.** En estas mediciones Spark fue "
                        f"más rápido incluso con el {p1:g} %, pero pandas es más simple de instalar y de depurar.")
        elif a and s:
            pequenos = (f"- **pandas sigue siendo la mejor opción con datos pequeños.** Con la muestra del {p1:g} % fue "
                        f"más rápido ({t(a['duracion_seg_media'])} frente a {t(s['duracion_seg_media'])}) y más simple.")
    if d and d.r.get("fuente") == "seccion5":
        factores = ("- **Hay factores que la medición no controla del todo.** Cada combinación se midió una vez, así que "
                    "no se puede estimar la variabilidad; la caché de disco del sistema operativo pudo favorecer a la "
                    "variante que leyó los archivos en segundo lugar.")
    else:
        factores = ("- **Hay factores que la medición no controla del todo.** La caché de disco del sistema operativo "
                    "favorece las lecturas repetidas, por eso se intercalaron las versiones.")
    return f"""## 6.6 Interpretación crítica y límites del análisis

Los resultados confirman lo que anticipaba el marco teórico, pero conviene precisar su alcance:

- **La ventaja de Spark en un solo equipo viene del paralelismo y del uso del disco, no de la distribución.** En modo `local[*]` no hay red ni varios nodos: Spark gana porque usa todos los núcleos y porque puede trabajar con más datos que su memoria. En un clúster el mismo código escalaría además horizontalmente, pero este trabajo no lo midió.
{pequenos} La conclusión no es que Spark sea mejor siempre, sino que hay un volumen a partir del cual pandas deja de ser viable en un equipo dado. Ese umbral depende de la RAM del equipo.
- **La comparación con pandas mitigado no es entre iguales.** Esa variante sacrifica exactitud (deduplicación local) para ahorrar memoria, así que un tiempo menor no significa un resultado equivalente. La equivalencia exacta entre pandas directo y Spark se verificó con `version_spark/validar_equivalencia.py` (sección 4.2.7).
{factores} El RSS de Windows no incluye la memoria enviada al archivo de paginación, de modo que el pico de pandas antes de fallar subestima lo que intentó reservar. Además, los datos son sintéticos: aunque se generaron con distribuciones realistas, un conjunto real podría tener otras proporciones de errores o de claves repetidas.
- **Spark también tiene límites en este equipo.** Con 4 GB asignados a la JVM, operaciones como el join con los 5 millones de clientes y la deduplicación global escriben datos intermedios en disco. Con un disco lento o sin espacio en `SPARK_LOCAL_DIRS`, el tiempo aumentaría o el proceso fallaría.
"""


def texto_conclusiones(d: Datos | None, comp: dict | None) -> str:
    # Cifras clave (si hay mediciones)
    if d:
        pmax = d.muestras[-1]
        sx = d.ok("spark", pmax)
        pd_ = d.r.get("pandas_directo", {})
        falla = pd_.get("primera_falla")
        frase_viab = (
            f"Con el conjunto completo ({num(sx['filas'] / 1e6, 2)} millones de filas, {num(sx['gb_csv'], 2)} GB), "
            f"Spark terminó en {t(sx['duracion_seg_media'])} con un pico de {num(sx['pico_ram_gb_media'], 2)} GB, "
            if sx else pend("tiempo y memoria de Spark con el 100 %") + " ")
        frase_viab += (
            f"mientras que pandas directo falló desde la muestra del {falla:g} % por "
            f"{FALLA.get(pd_.get('estado_primera_falla'), 'un error')}." if falla is not None
            else "y pandas directo también terminó. " + pend(
                "en estas mediciones pandas directo no falló; revisar las conclusiones 1 y 2 o verificar que "
                "las mediciones se hicieron en el equipo de 7,64 GB con el conjunto completo"))
        cruce = d.r.get("cruce_spark_vs_directo")
        frase_cruce = (f"a partir de la muestra del {cruce:g} % la relación se invierte" if cruce
                       else "en estas mediciones la relación no llegó a invertirse")
        s1, p1 = d.ok("spark", d.muestras[0]), d.ok("pandas_directo", d.muestras[0])
        pandas_gana = bool(s1 and p1 and p1["duracion_seg_media"] < s1["duracion_seg_media"])
        frase_pequenos = (f"(con el {d.muestras[0]:g} %, {t(p1['duracion_seg_media'])} en pandas frente a "
                          f"{t(s1['duracion_seg_media'])} en Spark)" if pandas_gana else "")
        nuc = [x for x in (d.media("spark", p, "nucleos_efectivos") for p in d.muestras if d.ok("spark", p))
               if x is not None]
        frase_cpu = (f"hasta {num(max(nuc), 1)} núcleos efectivos frente a uno solo en pandas" if nuc
                     else "todos los hilos del procesador con una tarea por partición, mientras que pandas usó uno solo")
    else:
        pandas_gana = True
        frase_viab = pend("cifras del benchmark")
        frase_cruce = "la relación se invierte al crecer el volumen"
        frase_pequenos = ""
        frase_cpu = "todos los núcleos del equipo"
    frase_codigo = ""
    if comp:
        tp, ts = comp["totales"]["pandas"], comp["totales"]["spark"]
        frase_codigo = (f" La complejidad ciclomática media por función fue {num(ts['cc_media_por_funcion'], 2)} en "
                        f"Spark frente a {num(tp['cc_media_por_funcion'], 2)} en pandas.")

    return f"""# 7. Conclusiones

1. **Importancia.** El proyecto mostró de forma experimental que existe un límite práctico para las herramientas de un solo proceso en memoria, y que ese límite lo fija el equipo, no el tamaño absoluto de los datos. {frase_viab} Para este problema, Spark no es una alternativa más rápida, sino la única forma viable de obtener el resultado completo en el mismo equipo.

2. **Eficiencia.** Spark fue más eficiente por tres mecanismos, y no por procesar cada fila más rápido:
   - **paralelismo:** aprovechó {frase_cpu};
   - **memoria acotada:** su consumo quedó limitado por `spark.driver.memory` y creció mucho menos que los datos, gracias al *spill* a disco;
   - **optimización del plan:** la evaluación perezosa permitió a Catalyst leer solo las columnas necesarias, aplicar los filtros temprano y elegir *broadcast join* para las dimensiones pequeñas.

   La eficiencia tiene un costo fijo: arrancar la JVM y planificar el DAG. {"Por eso pandas fue más rápido con datos pequeños " + frase_pequenos + " y " + frase_cruce + "." if pandas_gana else "En estas mediciones ese costo no alcanzó a superar la ventaja del paralelismo ni siquiera con la muestra más pequeña, pero con conjuntos aún menores pandas sería más rápido."}

3. **Aplicabilidad.** Spark conviene cuando los datos superan o se acercan a la memoria disponible, cuando el análisis requiere operaciones globales exactas (deduplicación, conteos distintos, rankings, joins entre tablas grandes) y cuando el volumen va a crecer. pandas sigue siendo preferible para exploración, prototipos y conjuntos que caben con holgura en memoria. Su API es más directa, no requiere Java ni configurar variables de entorno, y su sobrecarga es mínima. Una práctica razonable es combinar ambos: reducir los datos con Spark y continuar el análisis del resultado agregado, que es pequeño, en pandas.

4. **Utilidad y relación con casos reales.** El caso estudiado, consolidar años de transacciones de puntos de venta con los maestros de clientes, productos y tiendas, es representativo de lo que hacen a diario el comercio minorista, la banca o las telecomunicaciones. Zaharia et al. (2016) reportan miles de despliegues de Spark en aplicaciones que van de las finanzas al procesamiento de datos científicos, con clústeres públicos de más de 8.000 nodos. El código de este proyecto se ejecutaría sin cambios en un clúster de ese tipo, porque solo cambiaría el parámetro `master` de la sesión. Esa portabilidad es una ventaja práctica: el mismo programa sirve para desarrollar en un portátil y para producción.

5. **Complejidad del código.** Implementar las mitigaciones en pandas obligó a escribir un segundo pipeline más complejo y menos exacto, mientras que en Spark las operaciones globales son llamadas declarativas.{frase_codigo} El costo de Spark es otro: configurar el entorno, en especial en Windows (sección 3.7), y depurar planes que se ejecutan de forma perezosa.

6. **Limitaciones de Spark.** Tiene sobrecarga con datos pequeños, depende de la JVM y de una configuración delicada en Windows (versión de Java, `winutils.exe`, `hadoop.dll`, rutas sin espacios), consume memoria fija aunque los datos sean pocos y su rendimiento depende del disco cuando hay *spill*. Además, este trabajo evaluó solo el modo local en un equipo, con datos sintéticos. Queda como trabajo futuro medir el mismo código en un clúster de varios nodos y con datos reales.
"""


REFERENCIAS = """# 8. Referencias

::: {.referencias}
Apache Software Foundation. (2026). *Apache Spark 3.5.9* [Software]. https://spark.apache.org

Armbrust, M., Xin, R. S., Lian, C., Huai, Y., Liu, D., Bradley, J. K., Meng, X., Kaftan, T., Franklin, M. J., Ghodsi, A., & Zaharia, M. (2015). Spark SQL: Relational data processing in Spark. En *Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data* (pp. 1383–1394). Association for Computing Machinery. https://doi.org/10.1145/2723372.2742797

cdarlint. (s. f.). *winutils: Binarios de Hadoop para Windows* [Repositorio de GitHub]. https://github.com/cdarlint/winutils

Chambers, B., & Zaharia, M. (2018). *Spark: The definitive guide. Big data processing made simple*. O'Reilly Media.

Eclipse Adoptium. (s. f.). *Eclipse Temurin JDK 17* [Software]. https://adoptium.net

endoflife.date. (2026). *Apache Spark*. https://endoflife.date/apache-spark

Hunter, J. D. (2007). Matplotlib: A 2D graphics environment. *Computing in Science & Engineering, 9*(3), 90–95. https://doi.org/10.1109/MCSE.2007.55

Laney, D. (2001). *3D data management: Controlling data volume, velocity and variety*. META Group.

McCabe, T. J. (1976). A complexity measure. *IEEE Transactions on Software Engineering, SE-2*(4), 308–320. https://doi.org/10.1109/TSE.1976.233837

McKinney, W. (2017, 21 de septiembre). *Apache Arrow and the "10 things I hate about pandas"*. https://wesmckinney.com/blog/apache-arrow-pandas-internals/

McKinney, W. (2022). *Python for data analysis: Data wrangling with pandas, NumPy, and Jupyter* (3.ª ed.). O'Reilly Media.

Rodola, G. (2024). *psutil 6.1.1: Cross-platform lib for process and system monitoring in Python* [Software]. https://github.com/giampaolo/psutil

The pandas development team. (2024). *pandas 2.2.3* [Software]. Zenodo. https://doi.org/10.5281/zenodo.3509134

Western Digital. (2024). *PC SN5000S NVMe SSD specifications*. Western Digital Technologies.

Zaharia, M., Chowdhury, M., Das, T., Dave, A., Ma, J., McCauley, M., Franklin, M. J., Shenker, S., & Stoica, I. (2012). Resilient distributed datasets: A fault-tolerant abstraction for in-memory cluster computing. En *9th USENIX Symposium on Networked Systems Design and Implementation (NSDI 12)* (pp. 15–28). USENIX Association.

Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A., Meng, X., Rosen, J., Venkataraman, S., Franklin, M. J., Ghodsi, A., Gonzalez, J., Shenker, S., & Stoica, I. (2016). Apache Spark: A unified engine for big data processing. *Communications of the ACM, 59*(11), 56–65. https://doi.org/10.1145/2934664
:::
"""


def bloque_tabla(tablas: str | None, indice: int) -> str:
    """Devuelve la tabla `indice` (0, 1, 2) de tablas.md con su título y su nota."""
    if not tablas:
        return ""
    partes = [p for p in tablas.split("**Tabla ") if p.strip()]
    return "**Tabla " + partes[indice].strip() + "\n"


def main() -> None:
    import argparse
    global BENCH, SALIDA
    ap = argparse.ArgumentParser(description="Genera las secciones 6, 7 y 8 del informe")
    ap.add_argument("--bench", type=Path, default=BENCH, help="Carpeta con resumen.json, tablas.md y figuras")
    ap.add_argument("--salida", type=Path, default=SALIDA)
    args = ap.parse_args()
    BENCH, SALIDA = args.bench, args.salida
    resumen = BENCH / "resumen.json"
    d = Datos(json.loads(resumen.read_text(encoding="utf-8"))) if resumen.exists() else None
    tablas = (BENCH / "tablas.md").read_text(encoding="utf-8") if (BENCH / "tablas.md").exists() else None
    comp_json = BENCH / "complejidad_codigo.json"
    comp = json.loads(comp_json.read_text(encoding="utf-8")) if comp_json.exists() else None
    comp_md = (BENCH / "complejidad_codigo.md").read_text(encoding="utf-8") if comp else None
    params = d.r.get("parametros", {}) if d else {}

    aviso = ""
    if d and d.r.get("repeticiones_min", 0) < 3:
        aviso = pend(f"hay combinaciones con menos de 3 repeticiones (mínimo actual: {d.r.get('repeticiones_min')}); "
                     "completar con benchmark/medir.py") + "\n\n"

    global T_CODIGO
    if d and not d.r.get("hay_cpu", True):
        T_CODIGO = T_CPU          # sin tabla de CPU, la de complejidad pasa a ser la 13
        if comp_md:
            comp_md = re.sub(r"\*\*Tabla \d+\.\*\*", f"**Tabla {T_CODIGO}.**", comp_md, count=1)
    if d and d.r.get("fuente") == "seccion5":
        aviso = ""
    texto = "\n".join([
        "# 6. Análisis comparativo pandas vs. Spark\n",
        "Esta sección compara las dos versiones con métricas objetivas: tiempo de ejecución, memoria, CPU y "
        "escalabilidad (secciones 6.1 a 6.4), y complejidad y legibilidad del código (sección 6.5). La sección 5 "
        "documentó las fallas de pandas con capturas del sistema; aquí se cuantifican las tres variantes con "
        "mediciones repetidas en el mismo equipo.\n",
        aviso + texto_protocolo(d, params),
        texto_tiempo(d, tablas),
        texto_memoria(d, tablas),
        texto_cpu(d, tablas),
        texto_codigo(comp, comp_md),
        texto_discusion(d),
        texto_conclusiones(d, comp),
        REFERENCIAS,
    ])
    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    SALIDA.write_text(texto, encoding="utf-8")
    print(f"Secciones 6, 7 y 8 escritas en {SALIDA}")
    print("Con mediciones" if d else "SIN mediciones: el texto tiene marcas [PENDIENTE]")


if __name__ == "__main__":
    main()
