"""
benchmark/medir.py
==================
Benchmark comparativo pandas vs. PySpark (enunciado §4 y §6 del informe).

Mide, para cada versión y cada muestra (1 %, 10 %, 50 %, 100 %), con 3 repeticiones:
  - tiempo de ejecución de extremo a extremo (s);
  - pico de memoria RAM (RSS / working set) de TODO el árbol de procesos;
  - uso de CPU: promedio y pico (% del equipo) y núcleos efectivos (tiempo de CPU / tiempo real);
  - estado del sistema: pico de RAM usada (%) y de memoria de intercambio (swap / pagefile).

Versiones medidas (cada una es el `main.py` de su carpeta, sin modificar):
  pandas_directo   python version_pandas/main.py --muestra N
  pandas_mitigado  python version_pandas/main.py --muestra N --mitigado   (chunks + dtypes + usecols)
  spark            python version_spark/main.py  --muestra N

Decisiones de diseño (para que la comparación sea justa):
  * Cada repetición se lanza como un SUBPROCESO nuevo: pandas arranca un intérprete limpio y
    Spark una JVM limpia, y un MemoryError no tumba al medidor.
  * Se mide el árbol de procesos completo. En Spark el trabajo lo hace un proceso `java` hijo
    (lanzado por spark-submit); si solo se midiera el proceso Python, Spark aparentaría usar
    ~100 MB.
  * El tiempo incluye el arranque (intérprete, imports, JVM): es el tiempo que espera el usuario.
  * Las versiones se intercalan dentro de cada repetición (pandas_directo, pandas_mitigado,
    spark, pandas_directo, ...) para que la caché de disco del sistema operativo y la
    temperatura del equipo no favorezcan siempre a la misma versión.
  * Cada resultado se guarda en cuanto termina (JSONL). Si el equipo se bloquea o se cierra
    la terminal, al volver a ejecutar el script se retoma donde iba.
  * Si una versión falla (MemoryError o tiempo límite) en TODAS las repeticiones de una
    muestra, las muestras mayores de esa versión se registran como OMITIDO sin ejecutarlas:
    si no pudo con el 50 %, tampoco podrá con el 100 %. Se puede desactivar con --no-omitir.
  * Si una repetición agota el tiempo límite, las repeticiones restantes de esa misma
    combinación se registran como OMITIDO (cada una costaría otras horas). Se puede
    desactivar con --repetir-timeouts.

Uso (desde la raíz del proyecto, con el entorno virtual activado y SIN otras aplicaciones):
    python benchmark/medir.py                                  # todo: 3 versiones x 4 muestras x 3 rep.
    python benchmark/medir.py --muestras 1 10                  # solo algunas muestras
    python benchmark/medir.py --versiones spark --repeticiones 5
    python benchmark/medir.py --timeout 3600                   # límite por ejecución (s)
    python benchmark/medir.py --desde-cero                     # descarta mediciones anteriores
    python benchmark/medir.py --plan                           # muestra qué ejecutaría, sin ejecutar

Salida (carpeta evidencias/benchmark/):
    mediciones.jsonl        una línea JSON por ejecución (la fuente de la tabla y los gráficos)
    metadatos.json          equipo, versiones de software y parámetros del benchmark
    logs/<version>_<pct>pct_rep<k>.log   salida completa de cada ejecución (evidencia)

Después: python benchmark/graficos.py  (tablas y figuras del §6).
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import psutil

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from comun.muestras import PORCENTAJES_ESCALABILIDAD, resumen_muestra  # noqa: E402

DIR_SALIDA = RAIZ / "evidencias" / "benchmark"
ARCHIVO_MEDICIONES = DIR_SALIDA / "mediciones.jsonl"
GB = 1024**3

# Versión -> argumentos después de "python". {pct} se reemplaza por la muestra.
VERSIONES: dict[str, list[str]] = {
    "pandas_directo": ["version_pandas/main.py", "--muestra", "{pct}"],
    "pandas_mitigado": ["version_pandas/main.py", "--muestra", "{pct}", "--mitigado"],
    "spark": ["version_spark/main.py", "--muestra", "{pct}"],
}

# Texto en el log que identifica una falla por memoria (Python, NumPy, JVM, Windows).
PATRONES_MEMORIA = re.compile(
    r"MemoryError|Unable to allocate|OutOfMemoryError|Java heap space|"
    r"GC overhead limit exceeded|Cannot allocate memory|FALLA CR[IÍ]TICA POR MEMORYERROR|"
    r"paging file is too small|archivo de paginaci[oó]n es demasiado peque",
    re.IGNORECASE,
)


# ----------------------------------------------------------------------------
# Monitor del árbol de procesos
# ----------------------------------------------------------------------------
class MonitorArbol(threading.Thread):
    """Muestrea cada `intervalo` s la RAM y la CPU del proceso raíz y de todos sus hijos."""

    def __init__(self, pid: int, intervalo: float = 0.25):
        super().__init__(daemon=True)
        self.raiz = psutil.Process(pid)
        self.intervalo = intervalo
        self.detener_evento = threading.Event()
        self.n_cpus = psutil.cpu_count(logical=True) or 1
        self._procesos: dict[int, psutil.Process] = {}
        self._cpu_seg: dict[int, float] = {}       # último tiempo de CPU visto por pid
        self.muestras = 0
        self.pico_rss = 0
        self.pico_privada = 0                         # solo Windows: memoria confirmada (private bytes)
        self.pico_procesos = 0
        self.suma_cpu_pct = 0.0
        self.pico_cpu_pct = 0.0
        self.pico_ram_sistema_pct = 0.0
        self.min_ram_disponible = float("inf")
        self.pico_swap = 0
        self.swap_inicial = psutil.swap_memory().used

    def _arbol(self) -> list[psutil.Process]:
        try:
            actuales = [self.raiz] + self.raiz.children(recursive=True)
        except psutil.Error:
            return []
        vivos = []
        for p in actuales:
            # Reutilizar el objeto Process: cpu_percent() necesita la llamada anterior
            p = self._procesos.setdefault(p.pid, p)
            vivos.append(p)
        return vivos

    def run(self) -> None:
        while not self.detener_evento.is_set():
            rss = privada = 0
            cpu_pct = 0.0
            procesos = self._arbol()
            for p in procesos:
                try:
                    with p.oneshot():
                        mem = p.memory_info()
                        rss += mem.rss
                        privada += getattr(mem, "private", 0)
                        cpu_pct += p.cpu_percent(interval=None)
                        t = p.cpu_times()
                        self._cpu_seg[p.pid] = max(self._cpu_seg.get(p.pid, 0.0), t.user + t.system)
                except psutil.Error:
                    continue      # el proceso terminó entre la lista y la lectura
            vm = psutil.virtual_memory()
            sw = psutil.swap_memory()
            self.muestras += 1
            self.pico_rss = max(self.pico_rss, rss)
            self.pico_privada = max(self.pico_privada, privada)
            self.pico_procesos = max(self.pico_procesos, len(procesos))
            cpu_equipo = cpu_pct / self.n_cpus          # 100 % = todos los núcleos lógicos
            self.suma_cpu_pct += cpu_equipo
            self.pico_cpu_pct = max(self.pico_cpu_pct, cpu_equipo)
            self.pico_ram_sistema_pct = max(self.pico_ram_sistema_pct, vm.percent)
            self.min_ram_disponible = min(self.min_ram_disponible, vm.available)
            self.pico_swap = max(self.pico_swap, sw.used)
            self.detener_evento.wait(self.intervalo)

    def detener(self) -> dict:
        self.detener_evento.set()
        self.join(timeout=5)
        cpu_seg = sum(self._cpu_seg.values())
        return {
            "pico_ram_gb": round(self.pico_rss / GB, 3),
            "pico_memoria_privada_gb": round(self.pico_privada / GB, 3) if self.pico_privada else None,
            "cpu_promedio_pct": round(self.suma_cpu_pct / self.muestras, 1) if self.muestras else None,
            "cpu_pico_pct": round(self.pico_cpu_pct, 1),
            "tiempo_cpu_seg": round(cpu_seg, 2),
            "procesos_max": self.pico_procesos,
            "ram_sistema_pico_pct": round(self.pico_ram_sistema_pct, 1),
            "ram_sistema_min_disponible_gb": (round(self.min_ram_disponible / GB, 3)
                                              if self.min_ram_disponible != float("inf") else None),
            "swap_incremento_pico_gb": round(max(0, self.pico_swap - self.swap_inicial) / GB, 3),
            "muestras_monitor": self.muestras,
        }


def terminar_arbol(proceso: subprocess.Popen) -> None:
    """Termina el proceso y todos sus hijos (la JVM de Spark incluida)."""
    try:
        padre = psutil.Process(proceso.pid)
        hijos = padre.children(recursive=True)
    except psutil.Error:
        return
    for p in hijos + [padre]:
        try:
            p.kill()
        except psutil.Error:
            pass
    psutil.wait_procs(hijos + [padre], timeout=15)


# ----------------------------------------------------------------------------
# Una ejecución
# ----------------------------------------------------------------------------
def clasificar(codigo: int | None, timeout: bool, log: Path) -> tuple[str, str | None]:
    """Devuelve (estado, detalle) a partir del código de salida y del log."""
    if timeout:
        return "TIMEOUT", "Se alcanzó el tiempo límite y se detuvo el proceso"
    try:
        texto = log.read_text(encoding="utf-8", errors="replace")
    except OSError:
        texto = ""
    coincidencia = PATRONES_MEMORIA.search(texto)
    if codigo == 0:
        return "OK", None
    if codigo == 137 or coincidencia:
        return "MEMORY_ERROR", (coincidencia.group(0) if coincidencia else f"código de salida {codigo}")
    ultimas = [l for l in texto.strip().splitlines() if l.strip()][-1:]  # noqa: E741
    return "ERROR", f"código de salida {codigo}: {ultimas[0][:300] if ultimas else ''}"


def ejecutar_una(version: str, pct: float, rep: int, timeout_seg: float, intervalo: float,
                 args_extra: list[str]) -> dict:
    pct_txt = f"{pct:g}"
    comando = [sys.executable] + [a.replace("{pct}", pct_txt) for a in VERSIONES[version]] + args_extra
    log = DIR_SALIDA / "logs" / f"{version}_{pct_txt}pct_rep{rep}.log"
    log.parent.mkdir(parents=True, exist_ok=True)

    entorno = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUNBUFFERED="1")
    ram_disponible_antes = psutil.virtual_memory().available
    inicio = datetime.now()
    agotado = False

    with open(log, "w", encoding="utf-8") as salida:
        salida.write(f"# {' '.join(comando)}\n# inicio: {inicio.isoformat(timespec='seconds')}\n")
        salida.flush()
        t0 = time.perf_counter()
        proceso = subprocess.Popen(comando, cwd=RAIZ, stdout=salida, stderr=subprocess.STDOUT,
                                   env=entorno)
        monitor = MonitorArbol(proceso.pid, intervalo)
        monitor.start()
        try:
            codigo = proceso.wait(timeout=timeout_seg)
        except subprocess.TimeoutExpired:
            agotado = True
            terminar_arbol(proceso)
            codigo = proceso.wait()
        duracion = time.perf_counter() - t0
        recursos = monitor.detener()

    estado, detalle = clasificar(codigo, agotado, log)
    info = resumen_muestra(pct)
    nucleos = recursos["tiempo_cpu_seg"] / duracion if duracion > 0 else None
    return {
        "version": version,
        "muestra_pct": pct,
        "repeticion": rep,
        "estado": estado,
        "detalle": detalle,
        "codigo_salida": codigo,
        "duracion_seg": round(duracion, 3),
        **recursos,
        "nucleos_efectivos": round(nucleos, 2) if nucleos is not None else None,
        "ram_disponible_antes_gb": round(ram_disponible_antes / GB, 3),
        "archivos": info["archivos"],
        "filas": info["filas"],
        "gb_csv": info["gb"],
        "inicio": inicio.isoformat(timespec="seconds"),
        "comando": " ".join(Path(c).name if i == 0 else c for i, c in enumerate(comando)),
        "log": str(log.relative_to(RAIZ)),
    }


# ----------------------------------------------------------------------------
# Plan de ejecución, reanudación y metadatos
# ----------------------------------------------------------------------------
def leer_mediciones() -> list[dict]:
    if not ARCHIVO_MEDICIONES.exists():
        return []
    # Si una combinación aparece varias veces (p. ej. OMITIDO y luego medida), vale la última
    ultimas: dict[tuple, dict] = {}
    for linea in ARCHIVO_MEDICIONES.read_text(encoding="utf-8").splitlines():
        if linea.strip():
            r = json.loads(linea)
            ultimas[(r["version"], r["muestra_pct"], r["repeticion"])] = r
    return list(ultimas.values())


def guardar(registro: dict) -> None:
    DIR_SALIDA.mkdir(parents=True, exist_ok=True)
    with open(ARCHIVO_MEDICIONES, "a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=False) + "\n")


def omitido(version: str, pct: float, rep: int, motivo: str) -> dict:
    info = resumen_muestra(pct)
    return {"version": version, "muestra_pct": pct, "repeticion": rep, "estado": "OMITIDO",
            "detalle": motivo, "duracion_seg": None, "archivos": info["archivos"],
            "filas": info["filas"], "gb_csv": info["gb"],
            "inicio": datetime.now().isoformat(timespec="seconds")}


def guardar_metadatos(args: argparse.Namespace) -> None:
    """Ficha del equipo (scripts/info_equipo.py) + parámetros del benchmark."""
    # info_equipo.py escribe evidencias/info_equipo.{json,md}; se respaldan y se restauran para no
    # tocar la ficha que ya está en el informe. La ficha de ESTE benchmark queda en metadatos.json.
    equipo = {}
    fichas = [RAIZ / "evidencias" / f"info_equipo.{ext}" for ext in ("json", "md")]
    respaldo = {f: f.read_bytes() for f in fichas if f.exists()}
    try:
        subprocess.run([sys.executable, str(RAIZ / "scripts" / "info_equipo.py")], cwd=RAIZ,
                       capture_output=True, timeout=180,
                       env=dict(os.environ, PYTHONIOENCODING="utf-8"))
        equipo = json.loads(fichas[0].read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        equipo = {"error": f"No se pudo ejecutar info_equipo.py: {e}"}
    finally:
        for f in fichas:
            if f in respaldo:
                f.write_bytes(respaldo[f])
            elif f.exists():
                f.unlink()
    equipo.setdefault("ram_total_gb", round(psutil.virtual_memory().total / GB, 2))
    equipo.setdefault("hilos_logicos", psutil.cpu_count(logical=True))
    equipo.setdefault("nucleos_fisicos", psutil.cpu_count(logical=False))
    equipo.setdefault("sistema_operativo", platform.platform())

    try:
        manifiesto = json.loads((RAIZ / "datos" / "raw" / "manifiesto.json").read_text(encoding="utf-8"))
        datos = {k: manifiesto.get(k) for k in ("semilla", "parametros", "generado") if k in manifiesto}
        datos["transacciones"] = {k: v for k, v in manifiesto.get("transacciones", {}).items()
                                  if k != "detalle_partes"}
    except Exception:  # noqa: BLE001
        datos = None

    metadatos = {
        "fecha": datetime.now().isoformat(timespec="seconds"),
        "equipo": equipo,
        "datos": datos,
        "muestras": {f"{p:g}": resumen_muestra(p) for p in args.muestras},
        "parametros": {
            "versiones": args.versiones, "muestras": args.muestras,
            "repeticiones": args.repeticiones, "timeout_seg": args.timeout,
            "intervalo_monitor_seg": args.intervalo, "pausa_seg": args.pausa,
            "args_spark": args.args_spark, "omitir_tras_fallo": not args.no_omitir,
            "repetir_timeouts": args.repetir_timeouts,
            "python": sys.version.split()[0], "ejecutable": sys.executable,
        },
    }
    DIR_SALIDA.mkdir(parents=True, exist_ok=True)
    (DIR_SALIDA / "metadatos.json").write_text(json.dumps(metadatos, indent=2, ensure_ascii=False),
                                               encoding="utf-8")


def fallo_total(registros: list[dict], version: str, pct: float) -> bool:
    """True si la versión ejecutó esa muestra y ninguna repetición terminó bien."""
    propios = [r for r in registros if r["version"] == version and r["muestra_pct"] == pct]
    return bool(propios) and all(r["estado"] in ("MEMORY_ERROR", "TIMEOUT", "OMITIDO") for r in propios) \
        and any(r["estado"] in ("MEMORY_ERROR", "TIMEOUT") for r in propios)


def fmt_tiempo(seg: float | None) -> str:
    if seg is None:
        return "-"
    return f"{seg:.1f} s" if seg < 120 else f"{seg / 60:.1f} min"


def main() -> None:
    ap = argparse.ArgumentParser(description="Benchmark pandas vs. PySpark: tiempo, RAM y CPU")
    ap.add_argument("--versiones", nargs="+", choices=list(VERSIONES), default=list(VERSIONES))
    ap.add_argument("--muestras", nargs="+", type=float, default=[float(p) for p in PORCENTAJES_ESCALABILIDAD])
    ap.add_argument("--repeticiones", type=int, default=3)
    ap.add_argument("--timeout", type=float, default=7200,
                    help="Tiempo límite por ejecución en segundos (default 7200 = 2 h)")
    ap.add_argument("--intervalo", type=float, default=0.25, help="Intervalo de muestreo (s)")
    ap.add_argument("--pausa", type=float, default=10,
                    help="Pausa entre ejecuciones para que el SO libere memoria (s)")
    ap.add_argument("--args-spark", default="",
                    help='Argumentos extra para version_spark/main.py, p. ej. "--memoria-driver 4g"')
    ap.add_argument("--no-omitir", action="store_true",
                    help="Ejecuta las muestras mayores aunque la versión haya fallado en una menor")
    ap.add_argument("--repetir-timeouts", action="store_true",
                    help="Repite aunque una repetición anterior haya agotado el tiempo límite")
    ap.add_argument("--desde-cero", action="store_true",
                    help="Archiva mediciones.jsonl anterior y empieza de nuevo")
    ap.add_argument("--plan", action="store_true", help="Solo muestra el plan, no ejecuta")
    args = ap.parse_args()
    args.muestras = sorted(args.muestras)
    args_spark = args.args_spark.split()

    if args.desde_cero and ARCHIVO_MEDICIONES.exists():
        archivo = ARCHIVO_MEDICIONES.with_name(f"mediciones_{datetime.now():%Y%m%d_%H%M%S}.jsonl")
        shutil.move(ARCHIVO_MEDICIONES, archivo)
        print(f"Mediciones anteriores archivadas en {archivo.name}")

    # Los OMITIDO de una corrida anterior se vuelven a evaluar (p. ej. con --no-omitir)
    registros = [r for r in leer_mediciones() if r["estado"] != "OMITIDO"]
    hechos = {(r["version"], r["muestra_pct"], r["repeticion"]) for r in registros}
    pendientes = [(p, r, v) for p in args.muestras for r in range(1, args.repeticiones + 1)
                  for v in args.versiones if (v, p, r) not in hechos]

    print("=" * 78)
    print("BENCHMARK pandas vs. PySpark")
    print(f"Versiones: {', '.join(args.versiones)} | Muestras: {[f'{p:g} %' for p in args.muestras]}"
          f" | Repeticiones: {args.repeticiones} | Límite: {fmt_tiempo(args.timeout)}")
    print(f"Ya medidas: {len(hechos)} | Pendientes: {len(pendientes)} | Salida: {DIR_SALIDA}")
    print("=" * 78)
    if args.plan:
        for p, r, v in pendientes:
            print(f"  {v:16s} {p:>5g} %  rep {r}")
        return
    if not pendientes:
        print("No hay ejecuciones pendientes. Use --desde-cero para medir de nuevo.")
        return

    guardar_metadatos(args)
    vm = psutil.virtual_memory()
    print(f"RAM total {vm.total / GB:.2f} GB | disponible ahora {vm.available / GB:.2f} GB "
          f"({100 - vm.percent:.0f} %). Cierre otras aplicaciones para no sesgar la medición.\n")

    for i, (pct, rep, version) in enumerate(pendientes, 1):
        clave = (version, pct, rep)
        if clave in hechos:
            continue
        prefijo = f"[{i}/{len(pendientes)}] {version:16s} {pct:>5g} %  rep {rep}/{args.repeticiones}"

        # Regla 1: falló por completo en una muestra menor -> no tiene sentido una mayor
        menores = [m for m in args.muestras if m < pct]
        if not args.no_omitir and any(fallo_total(registros, version, m) for m in menores):
            m = max(m for m in menores if fallo_total(registros, version, m))
            reg = omitido(version, pct, rep, f"Falló en todas las repeticiones con la muestra del {m:g} %")
            print(f"{prefijo}  OMITIDO (falló con {m:g} %)")
        # Regla 2: una repetición anterior agotó el tiempo límite
        elif not args.repetir_timeouts and any(
                r["version"] == version and r["muestra_pct"] == pct and r["estado"] == "TIMEOUT"
                for r in registros):
            reg = omitido(version, pct, rep, "Una repetición anterior superó el tiempo límite")
            print(f"{prefijo}  OMITIDO (timeout previo)")
        else:
            print(f"{prefijo}  ...", end=" ", flush=True)
            reg = ejecutar_una(version, pct, rep, args.timeout, args.intervalo,
                               args_spark if version == "spark" else [])
            if reg["estado"] == "OK":
                print(f"OK  {fmt_tiempo(reg['duracion_seg'])} | RAM pico {reg['pico_ram_gb']:.2f} GB"
                      f" | CPU media {reg['cpu_promedio_pct']} % | {reg['nucleos_efectivos']} núcleos")
            else:
                print(f"{reg['estado']} tras {fmt_tiempo(reg['duracion_seg'])} "
                      f"(RAM pico {reg['pico_ram_gb']:.2f} GB) - {reg['detalle']}")
            time.sleep(args.pausa)

        registros.append(reg)
        hechos.add(clave)
        guardar(reg)

    print(f"\nListo. Mediciones en {ARCHIVO_MEDICIONES}")
    print("Siguiente paso: python benchmark/graficos.py")


if __name__ == "__main__":
    main()
