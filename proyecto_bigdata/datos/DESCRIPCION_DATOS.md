# Descripción del conjunto de datos

## 1. Caso de uso

**Dominio:** analítica de ventas de una cadena retail omnicanal en Colombia (supermercados,
hipermercados, tiendas express y venta en línea por web y app), periodo 2023‑2025.

**Problema de negocio.** Cada compra genera una fila en el sistema de punto de venta (POS) o en
la plataforma de comercio electrónico. La gerencia necesita consolidar esas transacciones con los
maestros de clientes, productos y tiendas para responder:

1. Ventas netas, unidades y margen bruto por **mes, región y categoría**.
2. **Tasa de devoluciones y cancelaciones** por canal y categoría.
3. **Top 10 productos** por departamento (ranking dentro de cada grupo).
4. **Ticket promedio y clientes únicos** por segmento de fidelización y mes.

Estas preguntas exigen leer, limpiar, unir (join) y agregar **toda** la historia de
transacciones, que es justamente lo que excede la memoria de un portátil.

## 2. Origen de los datos

Los datos son **sintéticos**: los genera `scripts/generar_datos.py` (código propio del equipo).
El enunciado permite datos sintéticos "siempre que su volumen sea realista". Se eligió esta opción
por cuatro razones:

| Criterio | Justificación |
|---|---|
| Volumen controlado | El tamaño se ajusta a la RAM del equipo de pruebas (por defecto 1,5 × RAM), así se garantiza que supera la memoria sin descargar decenas de GB. |
| Reproducibilidad | Con la misma semilla (`--semilla 2026`) y los mismos parámetros, cualquier integrante o el docente obtiene archivos idénticos. El repositorio entrega el **script** en vez de los datos (permitido en §5.3 del enunciado). |
| Operaciones requeridas | El modelo en estrella incluye por diseño los joins, la limpieza y las agregaciones que pide el enunciado. |
| Sin datos personales | No hay información real de personas, así que no hay restricciones de privacidad ni de licencia. |

El realismo se consigue con distribuciones tomadas del dominio: ciudades ponderadas por población,
popularidad de productos con ley de potencias (Zipf, pocos productos concentran muchas ventas),
estacionalidad (fines de semana, quincenas, noviembre y diciembre), horas pico, mezcla de canales y
medios de pago colombianos (efectivo, tarjetas, Nequi, Daviplata, PSE).

## 3. Modelo de datos (esquema en estrella)

```
                    ┌──────────────┐
                    │  productos   │  60.000 filas (~5 MB)
                    └──────┬───────┘
                           │ producto_id
┌──────────────┐   ┌───────┴──────────────┐   ┌──────────────┐
│   clientes   │───│    transacciones     │───│   tiendas    │
│ 5.000.000    │   │  (tabla de hechos)   │   │  600 filas   │
│ (~310 MB)    │   │ ~10,8 M filas por GB │   └──────────────┘
└──────────────┘   │ 100 archivos CSV     │     tienda_id
   cliente_id      └──────────────────────┘
   (LEFT: 8 % de compras son anónimas)
```

La diferencia de tamaño entre las dimensiones es intencional, para aplicar técnicas distintas de
optimización en Spark: `productos` y `tiendas` son pequeñas (*broadcast join*) y `clientes` es
grande (*sort‑merge join* con *shuffle*).

## 4. Diccionario de datos

### transacciones (`datos/raw/transacciones/part-XXXXX.csv`)

| Columna | Tipo | Descripción | Calidad / observaciones |
|---|---|---|---|
| transaccion_id | entero 64 | Identificador de la línea de venta | **0,3 % duplicados exactos** (reenvíos del POS) |
| fecha_hora | texto | Fecha y hora de la venta | 99,5 % `aaaa-mm-dd HH:MM:SS`; **0,5 % `dd/mm/aaaa HH:MM`**; 0,05 % nulos |
| cliente_id | entero | FK a clientes | **8 % nulo** (compra sin tarjeta de fidelización) |
| producto_id | entero | FK a productos | Popularidad sesgada (Zipf) |
| tienda_id | entero | FK a tiendas (para web/app es el punto de despacho) | — |
| canal | texto | `Tienda física` 70 %, `Web` 18 %, `App` 12 % | — |
| cantidad | entero | Unidades | **0,3 % ≤ 0** (errores de digitación) |
| precio_unitario | decimal | Precio cobrado en COP (lista ± 3 %) | **0,2 % nulo** |
| descuento_pct | entero | 0, 5, 10, 15, 20, 30 o 50 % | 75 % sin descuento |
| metodo_pago | texto | Efectivo, Tarjeta débito, Tarjeta crédito, Nequi, Daviplata, PSE | **3 % con variantes sucias** (`EFECTIVO`, `efectivo `, `Tarjeta Credito`…) |
| estado | texto | `COMPLETADA` 93 %, `DEVUELTA` 4 %, `CANCELADA` 3 % | — |

### clientes (`datos/raw/dimensiones/clientes.csv`)

| Columna | Tipo | Descripción |
|---|---|---|
| cliente_id | entero | Clave primaria |
| ciudad, departamento | texto | Residencia (25 ciudades capitales) |
| segmento | texto | Bronce 55 %, Plata 28 %, Oro 13 %, Platino 4 % |
| genero | texto | F / M / O |
| edad | entero | 18‑90; 4 % nulo |
| fecha_registro | fecha | Alta en el programa de fidelización |
| acepta_marketing | booleano | Consentimiento de comunicaciones |

### productos (`datos/raw/dimensiones/productos.csv`)

| Columna | Tipo | Descripción |
|---|---|---|
| producto_id | entero | Clave primaria |
| nombre_producto | texto | Subcategoría + marca + referencia |
| categoria, subcategoria | texto | 10 categorías, 45 subcategorías |
| marca | texto | 312 marcas |
| precio_lista | decimal | Precio de lista en COP (lognormal por categoría) |
| costo_unitario | decimal | Costo; margen típico por categoría (12 %‑45 %) |

### tiendas (`datos/raw/dimensiones/tiendas.csv`)

| Columna | Tipo | Descripción |
|---|---|---|
| tienda_id | entero | Clave primaria |
| nombre_tienda | texto | — |
| tipo_tienda | texto | Hipermercado, Supermercado, Express, Centro de distribución e‑commerce |
| ciudad, departamento, region | texto | Andina, Caribe, Pacífica, Orinoquía, Amazonía |
| fecha_apertura | fecha | — |
| area_m2 | entero | Tamaño; determina el peso de la tienda en las ventas |

## 5. Volumen

El tamaño de la tabla de hechos se calcula a partir de la RAM del equipo (~99 bytes por fila en CSV):

| RAM del equipo | Tamaño generado (1,5 × RAM) | Filas aprox. | Tamaño estimado en pandas* |
|---|---|---|---|
| 8 GB | 12 GB | ~130 millones | ~44 GB |
| 16 GB | 24 GB | ~260 millones | ~87 GB |
| 32 GB | 48 GB | ~520 millones | ~174 GB |

\* Medido con pandas 2.2.3 sobre una parte de 103 MB: `read_csv` sin opciones ocupa **3,63 ×** el
tamaño del CSV (`memory_usage(deep=True)`). Con tipos optimizados (`comun/esquemas.py`) baja a
**1,83 ×**, que sigue estando muy por encima de la RAM. Con pandas 3.x, que guarda los textos con
PyArrow, el factor medido fue 1,39 ×; el proyecto usa la rama 2.2 por compatibilidad con PySpark 3.5.

**Valores reales del equipo de pruebas:** ver `datos/raw/manifiesto.json` (filas, bytes, semilla y
tiempo de generación) y `evidencias/info_equipo.md`.

## 6. Muestras para el análisis de escalabilidad

Las transacciones se dividen en 100 archivos del mismo tamaño. La muestra del X % corresponde a los
primeros X archivos (`comun/muestras.py`), así que **pandas y Spark procesan exactamente las mismas
filas** en cada experimento:

| Muestra | Archivos | Tamaño con 16 GB de RAM |
|---|---|---|
| 1 % | part‑00000 | ~0,24 GB |
| 10 % | part‑00000 … 00009 | ~2,4 GB |
| 50 % | part‑00000 … 00049 | ~12 GB |
| 100 % | part‑00000 … 00099 | ~24 GB |

```python
from comun.muestras import rutas_muestra
rutas_muestra(10)   # -> lista de 10 rutas Path
```

## 7. Por qué la lectura por *chunks* no basta (mitigación de pandas)

El enunciado pide intentar primero las mitigaciones de pandas. Los datos se diseñaron para que esas
mitigaciones resuelvan una parte del problema, pero no todo:

| Operación | ¿Se resuelve por chunks? | Motivo |
|---|---|---|
| Suma de ventas por grupo | Sí | Las sumas parciales se pueden combinar |
| Eliminar duplicados | **No** | Los duplicados están mezclados al azar y caen en chunks distintos; deduplicar exige recordar ~260 M ids (un `set` de Python con esa cantidad ocupa decenas de GB) |
| Clientes únicos por mes (`nunique`) | **No** | Los conteos distintos parciales no se pueden sumar |
| Top 10 por departamento, mediana del ticket | **No** de forma exacta | Requieren ver toda la distribución del grupo |
| Join con `clientes` (5 M filas) | Parcial | La dimensión se repite en memoria en cada chunk, y el resultado del join sigue sin caber |

## 8. Cómo generar los datos

```powershell
# Desde la raíz del proyecto, con el entorno virtual activado
python scripts/generar_datos.py                       # 1,5 x RAM (recomendado para la entrega)
python scripts/generar_datos.py --tamano-gb 1 --partes 10 --forzar   # prueba rápida (~30 s)
```

El script comprueba antes que haya espacio libre suficiente (≈ 1,6 × el tamaño, para datos +
resultados Parquet + temporales de Spark) y usa todos los núcleos menos uno. El tiempo de generación
depende sobre todo del disco: en SSD, unos minutos para 24 GB.
