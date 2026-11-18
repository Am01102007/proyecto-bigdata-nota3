# Benchmark pandas vs. PySpark (§6 del informe)

**Autor:** Amed Santiago Vargas Zambrano

Mide tiempo, RAM y CPU de las tres variantes (pandas directo, pandas mitigado y PySpark)
con las muestras de 1 %, 10 %, 50 % y 100 %, 3 repeticiones cada una, y genera las tablas,
las figuras y el texto de las secciones 6, 7 y 8 del informe.

| Script | Qué hace |
|---|---|
| `medir.py` | Ejecuta cada combinación como subproceso y monitorea el árbol de procesos completo (Python + JVM de Spark). Guarda una línea por ejecución en `evidencias/benchmark/mediciones.jsonl` y el log completo en `evidencias/benchmark/logs/`. Si se interrumpe, al volver a ejecutarlo continúa donde iba. |
| `importar_seccion5.py` | Carga las mediciones de la sección 5 (una ejecución por caso) para generar el §6 mientras no se corra `medir.py`. Al correr `medir.py --desde-cero` se archivan y se reemplazan. |
| `graficos.py` | Calcula media, desviación estándar, mínimo y máximo, y genera `tablas.md`, los CSV y las figuras `fig_tiempo.png`, `fig_memoria.png` y `fig_cpu.png`. |
| `complejidad_codigo.py` | Líneas de código efectivas, funciones y complejidad ciclomática de cada etapa de las dos versiones. |
| `../informe/generar_secciones.py` | Escribe `informe/secciones_6_7_8.md` con las cifras de los pasos anteriores. |
| `../informe/ensamblar_pdf.py` | Arma el PDF final (requiere pandoc y Chrome o Edge). |

## 1. Antes de medir (en el equipo de pruebas)

Las mediciones tienen que hacerse en **el mismo equipo** de las secciones 3 y 5 del informe (el
portátil de 7,64 GB), con el entorno de la Tabla 6 (JDK 17, Python 3.11, pandas 2.2.3, PySpark 3.5.9).

1. Conecte el portátil a la corriente y cierre las demás aplicaciones, incluido el navegador.
   En Windows, active el plan de energía *Máximo rendimiento* o *Equilibrado*, pero no *Ahorro*.
2. Compruebe el entorno y los datos:
   ```powershell
   .\.venv\Scripts\Activate.ps1
   python scripts\verificar_entorno.py
   python comun\muestras.py          # deben aparecer las 4 muestras con sus filas y GB
   ```
3. Vea el plan sin ejecutar nada:
   ```powershell
   python benchmark\medir.py --plan
   ```

## 2. Medir

```powershell
python benchmark\medir.py
```

- Son 36 ejecuciones: 3 versiones × 4 muestras × 3 repeticiones. En el portátil de 8 GB pueden
  tardar entre 2 y 4 horas. Lo que más demora es pandas mitigado con 50 % y 100 %.
- El límite por ejecución es de 2 horas (`--timeout 7200`). Si pandas falla con una muestra en las
  3 repeticiones, sus muestras mayores se marcan como *no ejecutadas*, sin gastar horas en ellas.
- Si el equipo se bloquea por falta de memoria, que es justo lo que se quiere demostrar, espere a
  que el proceso falle o reinicie el equipo. Después vuelva a ejecutar `python benchmark\medir.py`
  y continuará desde la ejecución pendiente.
- Durante pandas directo con 50 % y 100 %, tome capturas del Administrador de tareas para la §5.
- Opciones útiles: `--muestras 1 10`, `--versiones spark`, `--repeticiones 5`,
  `--args-spark "--memoria-driver 4g"` y `--desde-cero` (archiva las mediciones anteriores).

## 3. Tablas, figuras e informe

```powershell
python benchmark\graficos.py
python benchmark\complejidad_codigo.py
python informe\generar_secciones.py
python informe\ensamblar_pdf.py        # opcional: necesita pandoc (winget install JohnMacFarlane.Pandoc)
```

Para entregar, suba al repositorio (o comparta) la carpeta `evidencias\benchmark\` completa:
`mediciones.jsonl`, `metadatos.json`, `logs\` y lo que generó `graficos.py`.

## 4. Qué se mide exactamente

| Métrica | Cómo se obtiene |
|---|---|
| Tiempo | Reloj de pared desde que se lanza el proceso hasta que termina. Incluye el arranque de Python y de la JVM. |
| Pico de RAM | Máximo de la suma del RSS (*working set* en Windows) del proceso y de todos sus hijos, muestreado cada 0,25 s. En Spark la JVM es un proceso hijo: si solo se midiera Python, Spark aparentaría usar unos 100 MB. |
| RAM del sistema | Pico de `psutil.virtual_memory().percent` durante la ejecución (incluye al sistema operativo). |
| CPU promedio | Suma del `cpu_percent` de los procesos del árbol dividida entre el número de hilos lógicos (100 % = equipo completo). |
| Núcleos efectivos | Tiempo de CPU (usuario + sistema) del árbol dividido entre el tiempo real. pandas ≈ 1; Spark puede llegar al número de hilos. |
| Estado | `OK`, `MEMORY_ERROR` (código 137 o `MemoryError`/`OutOfMemoryError` en el log), `TIMEOUT`, `ERROR` u `OMITIDO`. |
