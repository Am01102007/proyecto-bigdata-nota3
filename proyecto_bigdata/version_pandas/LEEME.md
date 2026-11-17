# Versión pandas - Proyecto Nota 3 (Big Data)

**Autor:** Samuel Zuluaga  
**Módulo:** `version_pandas`  
**Objetivo:** Demostración de inviabilidad técnica y analítica de pandas frente a volúmenes masivos de datos (superiores a la memoria RAM) y comparación frente a Apache Spark.

---

## 1. Estructura de la solución

```
version_pandas/
├── LEEME.md                    <- este documento
├── __init__.py                 <- inicialización del paquete
├── cargador.py                 <- lectura de dimensiones y transacciones (directo y por chunks)
├── limpieza.py                 <- normalización de fechas, métodos de pago, filtros y deduplicación
├── joins.py                    <- cruces relacionales del modelo en estrella (productos, tiendas, clientes)
├── agregaciones.py             <- cálculo de las 4 consultas analíticas de negocio
├── escritura.py                <- persistencia en formato columnar Parquet (resultados/pandas/)
├── mitigaciones.py             <- pipeline por fragmentos (chunks), dtypes optimizados y usecols
├── main.py                     <- punto de entrada CLI para ejecución y pruebas
└── benchmark_inviabilidad.py   <- suite de benchmark automatizada con monitoreo de RAM y CPU
```

---

## 2. Instrucciones de ejecución

Asegúrese de encontrarse en la raíz del proyecto (`proyecto_bigdata`) con el entorno virtual activado:

```powershell
# 1. Ejecución estándar con muestra del 1 % (modo directo):
python version_pandas/main.py --muestra 1

# 2. Ejecución con muestra del 10 % con técnicas de mitigación (chunks + dtypes + usecols):
python version_pandas/main.py --muestra 10 --mitigado

# 3. Prueba de estrés con muestra del 50 % mitigada:
python version_pandas/main.py --muestra 50 --mitigado --chunksize 150000

# 4. Prueba del conjunto completo (100 %) para observar límites de memoria:
python version_pandas/main.py --muestra 100 --mitigado

# 5. Suite completa de benchmarking y registro de inviabilidad (§5 del informe):
python version_pandas/benchmark_inviabilidad.py
```

---

## 3. Consultas de negocio implementadas

1. **Consulta 1:** Ventas netas, unidades y margen bruto por mes (`anio_mes`), región geográfica y categoría.
2. **Consulta 2:** Tasa porcentual de devoluciones y cancelaciones por canal de venta (`Tienda física`, `Web`, `App`) y categoría de producto.
3. **Consulta 3:** Top 10 productos más vendidos dentro de cada departamento (ranking intra-grupo).
4. **Consulta 4:** Ticket promedio y cardinalidad de clientes únicos (`nunique`) por segmento de fidelización (`Bronce`, `Plata`, `Oro`, `Platino`, `Sin fidelización`) y mes.

---

## 4. Técnicas de mitigación evaluadas y sus limitaciones

El enunciado (§4.2) exige probar y documentar las tres mitigaciones estándar de pandas:
1. **Tipos de datos optimizados (`dtypes`):** Uso de `category` para dimensiones de baja cardinalidad, `int8` para porcentajes de descuento, `int16` para cantidades y tiendas, `float32` para montos monetarios e `Int32` nullable para llaves foráneas. Reduce la huella en memoria aproximadamente un **50 %** respecto a los tipos por defecto (`object`, `int64`, `float64`), pero sigue siendo insuficiente al procesar decenas de gigabytes.
2. **Selección selectiva de columnas (`usecols`):** Carga únicamente las columnas requeridas para cada agregación en lugar de las 11 columnas de la tabla de hechos.
3. **Lectura por fragmentos (`chunksize`):** Procesa el flujo transaccional en streaming local.

### ¿Por qué las mitigaciones no son suficientes?
- **Deduplicación global imposible en streaming:** Al estar los duplicados repartidos al azar entre archivos y fragmentos, un `drop_duplicates` local solo detecta duplicados dentro del mismo bloque. Una deduplicación global exacta exige almacenar el conjunto de todos los identificadores en memoria, agotando la RAM.
- **Conteo de clientes únicos no combinable:** `nunique()` es una métrica no distributiva ($|A \cup B| \neq |A| + |B|$). No se puede calcular sumando los conteos de cada chunk sin retener en memoria los conjuntos de clientes.
- **Sobrecarga de CPU y GIL:** Al ser monocore, pandas no aprovecha los múltiples núcleos del procesador moderno (Intel Core i5 con 8 núcleos / 12 hilos), tardando horas frente a los segundos/minutos de Spark.
- **Desbordamiento de memoria (`MemoryError`):** Al procesar el 50 % o 100 % del dataset, los cruces con el maestro de clientes (5 millones de filas) y las tablas intermedias superan los 8 GB físicos del equipo de trabajo, forzando swap masivo en disco y el colapso del proceso.
