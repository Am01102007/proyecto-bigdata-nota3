# Versión PySpark (a cargo de otro integrante)

```python
import sys; from pathlib import Path; sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.muestras import rutas_muestra, DIR_DIMENSIONES
from comun.esquemas import esquema_spark_transacciones   # evita inferSchema (una lectura completa extra)

tx = spark.read.csv([str(p) for p in rutas_muestra(10)], header=True,
                    schema=esquema_spark_transacciones())
```

Sugerencias de optimización según el diseño de los datos: `broadcast` de productos y tiendas,
join normal con clientes (5 M filas), conversión a Parquet particionado por año/mes, `cache()`
del DataFrame limpio si se reutiliza en varias agregaciones. Configuración de memoria: README §5.
