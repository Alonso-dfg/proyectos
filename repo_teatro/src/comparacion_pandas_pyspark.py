"""
Comparación Pandas vs. PySpark sobre el mismo indicador
=======================================================

Responde el reto del corte: "¿qué ventaja aportaría Spark si el archivo
tuviera decenas de millones de registros y qué costos habría que considerar?"

Se calcula el MISMO indicador (ingreso y boletas por obra, solo entradas con
precio >= 25.000) con Pandas y con PySpark sobre la capa STAGING replicada
x1, x100 y x1000 para simular crecimiento del volumen. Se verifica que ambos
den el mismo resultado y se registran los tiempos.

Ejecución:  python3 src/comparacion_pandas_pyspark.py
Requiere haber ejecutado antes src/pipeline_batch_spark.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
from pyspark.sql import functions as F

from comun import LOGS, STAGING, STREAM, crear_spark

FACTORES = [1, 100, 1000]
UMBRAL_PRECIO = 25000


def indicador_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    filtrado = pdf[pdf["precio"] >= UMBRAL_PRECIO]
    return (filtrado.groupby("obra")
            .agg(boletas=("id_venta", "count"), ingreso_cop=("precio", "sum"))
            .sort_values("ingreso_cop", ascending=False).reset_index())


def indicador_spark(sdf):
    return (sdf.filter(F.col("precio") >= UMBRAL_PRECIO)
            .groupBy("obra")
            .agg(F.count("*").alias("boletas"),
                 F.sum("precio").alias("ingreso_cop"))
            .orderBy(F.desc("ingreso_cop")))


def main() -> None:
    spark = crear_spark("TeatroComparacion")
    base = spark.read.parquet(str(STAGING / "ventas_boletas")).select(
        "id_venta", "obra", "precio")
    base_pd = base.toPandas()

    filas = []
    for factor in FACTORES:
        # Datos escalados: se guarda en Parquet para que ambos lean de disco
        ruta = STREAM.parent / "tmp_escalado" / f"x{factor}"
        escalado = base.crossJoin(spark.range(factor).withColumnRenamed(
            "id", "replica"))
        escalado.write.mode("overwrite").parquet(str(ruta))
        n = spark.read.parquet(str(ruta)).count()

        t0 = time.perf_counter()
        pdf = pd.read_parquet(ruta)
        res_pd = indicador_pandas(pdf)
        t_pd = time.perf_counter() - t0

        t0 = time.perf_counter()
        sdf = spark.read.parquet(str(ruta))
        res_sp = indicador_spark(sdf).toPandas()
        t_sp = time.perf_counter() - t0

        iguales = (res_pd["ingreso_cop"].astype("int64").tolist()
                   == res_sp["ingreso_cop"].astype("int64").tolist())
        filas.append({"factor": f"x{factor}", "registros": n,
                      "pandas_s": round(t_pd, 3), "pyspark_s": round(t_sp, 3),
                      "mismo_resultado": iguales})
        print(f"x{factor:<5} {n:>10,} filas | Pandas {t_pd:6.3f} s | "
              f"PySpark {t_sp:6.3f} s | resultados iguales: {iguales}")

    print("\nResultado del indicador (x1):")
    print(indicador_pandas(base_pd).to_string(index=False))

    tabla = pd.DataFrame(filas)
    LOGS.mkdir(parents=True, exist_ok=True)
    tabla.to_csv(LOGS / "comparacion_pandas_pyspark.csv", index=False)
    print("\nTabla guardada en evidencia/logs_calidad/"
          "comparacion_pandas_pyspark.csv")
    print("Lectura: con pocos datos Pandas es más rápido (Spark paga el costo "
          "de planificar y coordinar tareas). La ventaja de Spark aparece "
          "cuando el volumen supera la memoria de un equipo o se ejecuta en "
          "un clúster con varios executors.")
    spark.stop()


if __name__ == "__main__":
    main()
