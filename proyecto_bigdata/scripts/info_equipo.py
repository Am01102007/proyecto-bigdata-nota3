"""
info_equipo.py
==============
Registra el hardware y el software del equipo de pruebas. El enunciado exige
"ejecutar ambas versiones en el mismo equipo ... indicando procesador, RAM,
disco y versiones de software". Este script genera esa ficha técnica.

Uso:
    python scripts/info_equipo.py
Salida:
    evidencias/info_equipo.json   (para los scripts de benchmark)
    evidencias/info_equipo.md     (tabla lista para pegar en el informe, §3)
"""

from __future__ import annotations

import importlib
import json
import os
import platform
import re
import subprocess
from datetime import datetime
from pathlib import Path

import psutil

RAIZ = Path(__file__).resolve().parents[1]
ES_WINDOWS = platform.system() == "Windows"


def _ps(comando: str) -> str:
    """Ejecuta un comando de PowerShell y devuelve su salida (solo Windows)."""
    try:
        r = subprocess.run(["powershell", "-NoProfile", "-Command", comando],
                           capture_output=True, text=True, timeout=60)
        return r.stdout.strip()
    except Exception:  # noqa: BLE001
        return ""


def procesador() -> str:
    if ES_WINDOWS:
        nombre = _ps("(Get-CimInstance Win32_Processor).Name")
        if nombre:
            return nombre
    try:
        for linea in Path("/proc/cpuinfo").read_text().splitlines():
            if linea.startswith("model name"):
                return linea.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "desconocido"


def discos() -> list[dict]:
    datos = []
    if ES_WINDOWS:
        salida = _ps("Get-PhysicalDisk | Select-Object FriendlyName,MediaType,BusType,"
                     "@{n='GB';e={[math]::Round($_.Size/1GB)}} | ConvertTo-Json")
        try:
            d = json.loads(salida) if salida else []
            datos = d if isinstance(d, list) else [d]
        except json.JSONDecodeError:
            pass
    return datos


def version_java() -> str:
    jh = os.environ.get("JAVA_HOME")
    exe = str(Path(jh) / "bin" / "java") if jh else "java"
    try:
        r = subprocess.run([exe, "-version"], capture_output=True, text=True, timeout=30)
        return (r.stderr or r.stdout).splitlines()[0]
    except Exception:  # noqa: BLE001
        return "no encontrado"


def version_lib(nombre: str) -> str:
    try:
        return importlib.import_module(nombre).__version__
    except Exception:  # noqa: BLE001
        return "no instalado"


def main() -> None:
    vm = psutil.virtual_memory()
    unidad = RAIZ.anchor or "/"
    uso = psutil.disk_usage(unidad)
    freq = psutil.cpu_freq()
    info = {
        "fecha": datetime.now().isoformat(timespec="seconds"),
        "equipo": platform.node(),
        "sistema_operativo": f"{platform.system()} {platform.release()} (build {platform.version()})",
        "procesador": procesador(),
        "nucleos_fisicos": psutil.cpu_count(logical=False),
        "hilos_logicos": psutil.cpu_count(logical=True),
        "frecuencia_max_mhz": round(freq.max) if freq else None,
        "ram_total_gb": round(vm.total / 1024**3, 2),
        "ram_disponible_gb_al_medir": round(vm.available / 1024**3, 2),
        "disco_unidad_proyecto": unidad,
        "disco_total_gb": round(uso.total / 1024**3, 1),
        "disco_libre_gb": round(uso.free / 1024**3, 1),
        "discos_fisicos": discos(),
        "python": platform.python_version(),
        "java": version_java(),
        "pyspark": version_lib("pyspark"),
        "pandas": version_lib("pandas"),
        "numpy": version_lib("numpy"),
        "pyarrow": version_lib("pyarrow"),
        "JAVA_HOME": os.environ.get("JAVA_HOME"),
        "SPARK_HOME": os.environ.get("SPARK_HOME"),
        "HADOOP_HOME": os.environ.get("HADOOP_HOME"),
        "PYSPARK_PYTHON": os.environ.get("PYSPARK_PYTHON"),
    }

    destino = RAIZ / "evidencias"
    destino.mkdir(exist_ok=True)
    (destino / "info_equipo.json").write_text(json.dumps(info, indent=2, ensure_ascii=False),
                                              encoding="utf-8")

    disco_txt = "; ".join(f"{d.get('FriendlyName')} ({d.get('MediaType')}, {d.get('BusType')}, "
                          f"{d.get('GB')} GB)" for d in info["discos_fisicos"]) or "ver Administrador de tareas"
    filas = [
        ("Sistema operativo", info["sistema_operativo"]),
        ("Procesador", f"{info['procesador']} - {info['nucleos_fisicos']} núcleos / "
                       f"{info['hilos_logicos']} hilos"),
        ("Memoria RAM", f"{info['ram_total_gb']} GB"),
        ("Disco", f"{disco_txt}; libre en {unidad}: {info['disco_libre_gb']} GB"),
        ("Python", info["python"]),
        ("Java (JDK)", info["java"]),
        ("Apache Spark / PySpark", info["pyspark"]),
        ("pandas / NumPy / PyArrow", f"{info['pandas']} / {info['numpy']} / {info['pyarrow']}"),
    ]
    md = "| Componente | Valor |\n|---|---|\n" + "\n".join(f"| {a} | {b} |" for a, b in filas)
    md += "\n\n| Variable | Valor |\n|---|---|\n" + "\n".join(
        f"| {k} | `{info[k]}` |" for k in ["JAVA_HOME", "SPARK_HOME", "HADOOP_HOME", "PYSPARK_PYTHON"])
    (destino / "info_equipo.md").write_text(md + "\n", encoding="utf-8")
    print(md)
    print(f"\nGuardado en {destino / 'info_equipo.json'} y {destino / 'info_equipo.md'}")


if __name__ == "__main__":
    main()
