"""
Pipeline batch con PySpark: RAW -> STAGING (silver) -> GOLD (modelo estrella)
=============================================================================

Proyecto integrador · Gestión de venta de boletas de un teatro
Ingeniería de Datos · Procesamiento distribuido con Spark (modo batch)

Qué hace
--------
1. Lee el archivo RAW con un ESQUEMA EXPLÍCITO (no se infiere nada) y le
   agrega metadatos de linaje (archivo de origen, fecha de proceso, versión
   del contrato).
2. Aplica el contrato de datos (src/contrato.py, docs/contrato_datos.md)
   con TRANSFORMACIONES de Spark: normaliza obra y fecha, imputa canal vacío,
   marca reglas incumplidas y elimina duplicados de id_venta.
3. Separa los registros válidos (STAGING) de los que requieren revisión
   manual (CUARENTENA), sin borrar nada: la trazabilidad se conserva.
4. Construye la capa GOLD: hecho_venta_boleta + 4 dimensiones, y calcula los
   indicadores que responden las preguntas analíticas del proyecto.
5. Deja evidencia: log de calidad (JSON), plan físico de Spark (explain) y
   tablas GOLD exportadas a CSV para consulta.

Ejecución (desde la raíz del repositorio):
    python3 src/pipeline_batch_spark.py
"""
import json
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from comun import (CANAL_NO_ESPECIFICADO, CANALES, CATALOGO_FUNCIONES,
                   CUARENTENA, GOLD, LOGS, RAW_CSV, STAGING, TIPOS_ENTRADA,
                   crear_spark)
from contrato import VERSION_CONTRATO, aplicar_contrato

# El RAW se lee TODO como texto: así ningún valor se pierde por un error de
# tipo al leer y las validaciones quedan explícitas en el código.
ESQUEMA_RAW = StructType([
    StructField("id_venta", StringType(), True),
    StructField("id_funcion", StringType(), True),
    StructField("obra", StringType(), True),
    StructField("fecha_funcion", StringType(), True),
    StructField("hora_funcion", StringType(), True),
    StructField("tipo_entrada", StringType(), True),
    StructField("canal_venta", StringType(), True),
    StructField("precio", StringType(), True),
])

ESQUEMA_CATALOGO = StructType([
    StructField("id_funcion", IntegerType(), False),
    StructField("obra", StringType(), False),
    StructField("fecha_funcion", StringType(), False),
    StructField("hora_funcion", StringType(), False),
    StructField("sala", StringType(), False),
    StructField("aforo", IntegerType(), False),
])


def titulo(texto: str) -> None:
    print("\n" + "=" * 78 + f"\n{texto}\n" + "=" * 78)


def guardar_csv(df, nombre: str) -> None:
    """Exporta una tabla GOLD pequeña a un único CSV legible (para el informe).

    toPandas() es una ACCIÓN que trae los datos al Driver: solo se usa aquí
    porque las tablas GOLD son agregados pequeños. Con datos grandes se
    escribiría con df.write.csv(...) directamente desde los executors.
    """
    (GOLD / "csv").mkdir(parents=True, exist_ok=True)
    df.toPandas().to_csv(GOLD / "csv" / f"{nombre}.csv", index=False)


def main() -> None:
    inicio = time.time()
    spark = crear_spark("TeatroPipelineBatch")
    fecha_proceso = datetime.now().isoformat(timespec="seconds")

    # ------------------------------------------------------------------
    # 1. RAW (bronze): lectura con esquema explícito + linaje
    # ------------------------------------------------------------------
    titulo("1. Lectura de la capa RAW")
    raw = (spark.read
           .option("header", True)
           .schema(ESQUEMA_RAW)
           .csv(str(RAW_CSV))
           .withColumn("_archivo_origen", F.input_file_name())
           .withColumn("_fecha_proceso", F.lit(fecha_proceso))
           .withColumn("_version_contrato", F.lit(VERSION_CONTRATO)))

    total_raw = raw.count()                       # ACCIÓN -> ejecuta lectura
    print(f"Registros RAW: {total_raw}")
    print(f"Particiones del DataFrame RAW: {raw.rdd.getNumPartitions()}")
    raw.printSchema()

    # ------------------------------------------------------------------
    # 2. Transformaciones del contrato (todas son LAZY: no se ejecutan aún)
    # ------------------------------------------------------------------
    titulo("2. Aplicación del contrato de datos (transformaciones)")

    # Las reglas viven en src/contrato.py: el consumidor de streaming usa
    # exactamente las mismas funciones (un solo contrato para ambos modos).
    t0 = time.time()
    df = aplicar_contrato(raw)

    # Unicidad: se numeran las filas por id_venta; la 1.a se conserva.
    ventana_id = Window.partitionBy("id_venta").orderBy(
        F.col("_archivo_origen"), F.col("fecha_original"))
    df = df.withColumn("n_ocurrencia", F.row_number().over(ventana_id))
    print(f"Transformaciones definidas en {time.time() - t0:.3f} s "
          "(lazy evaluation: aún no se ha procesado ningún dato)")

    # cache(): el DataFrame se reutiliza varias veces abajo.
    df = df.cache()

    duplicados = df.filter(F.col("n_ocurrencia") > 1)
    unicos = df.filter(F.col("n_ocurrencia") == 1)
    cuarentena = unicos.filter(F.size("motivos_cuarentena") > 0)
    staging = unicos.filter(F.size("motivos_cuarentena") == 0)

    # ------------------------------------------------------------------
    # 3. Escritura STAGING + CUARENTENA y log de calidad
    # ------------------------------------------------------------------
    titulo("3. Escritura de STAGING (silver) y CUARENTENA")
    columnas_staging = ["id_venta", "id_funcion", "obra", "fecha_funcion",
                        "hora_funcion", "tipo_entrada", "canal_venta",
                        "precio", "obra_original", "fecha_original",
                        "fecha_corregida", "canal_imputado",
                        "_archivo_origen", "_fecha_proceso",
                        "_version_contrato"]

    # Particionar por fecha: cada función queda en su carpeta y una consulta
    # por fecha solo lee la partición necesaria (partition pruning).
    (staging.select(*columnas_staging)
     .write.mode("overwrite")
     .partitionBy("fecha_funcion")
     .parquet(str(STAGING / "ventas_boletas")))

    (cuarentena
     .select(*columnas_staging, F.concat_ws("|", "motivos_cuarentena")
             .alias("motivos_cuarentena"))
     .write.mode("overwrite")
     .parquet(str(CUARENTENA / "ventas_boletas")))
    (duplicados.select(*columnas_staging)
     .write.mode("overwrite").parquet(str(CUARENTENA / "duplicados")))

    conteo_motivos = {
        r["motivo"]: r["n"] for r in
        cuarentena.select(F.explode("motivos_cuarentena").alias("motivo"))
        .groupBy("motivo").agg(F.count("*").alias("n")).collect()
    }
    n_staging = staging.count()
    n_cuarentena = cuarentena.count()
    n_duplicados = duplicados.count()
    log = {
        "fecha_proceso": fecha_proceso,
        "version_contrato": VERSION_CONTRATO,
        "archivo_origen": str(RAW_CSV.name),
        "registros_raw": total_raw,
        "filas_duplicadas_descartadas": n_duplicados,
        "registros_cuarentena": n_cuarentena,
        "motivos_cuarentena": conteo_motivos,
        "registros_staging": n_staging,
        "correcciones_aplicadas": {
            "fechas_dd_mm_aaaa_normalizadas":
                staging.filter("fecha_corregida").count(),
            "obras_normalizadas_contra_catalogo":
                staging.filter(F.col("obra") != F.col("obra_original")).count(),
            "canal_imputado_no_especificado":
                staging.filter("canal_imputado").count(),
        },
        "cuadre": total_raw == n_staging + n_cuarentena + n_duplicados,
    }
    LOGS.mkdir(parents=True, exist_ok=True)
    (LOGS / "log_calidad_batch.json").write_text(
        json.dumps(log, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(log, indent=2, ensure_ascii=False))

    # Umbrales del contrato antes de promover a GOLD
    titulo("Verificación de umbrales del contrato sobre STAGING")
    verif = staging.agg(
        (F.count("*") - F.countDistinct("id_venta")).alias("dup_id"),
        F.sum(F.col("fecha_funcion").isNull().cast("int")).alias("fecha_nula"),
        F.sum(((F.col("tipo_entrada") != "Cortesia")
               & (F.col("precio") == 0)).cast("int")).alias("precio_cero"),
        F.avg(F.col("canal_imputado").cast("int")).alias("pct_canal_vacio"),
    ).first()
    umbrales = {
        "Unicidad (0 duplicados de id_venta)": verif["dup_id"] == 0,
        "Validez (100% fechas AAAA-MM-DD)": verif["fecha_nula"] == 0,
        "Consistencia (0 precio=0 sin cortesía)": verif["precio_cero"] == 0,
        f"Completitud canal (<2% vacíos; actual "
        f"{verif['pct_canal_vacio'] * 100:.1f}%)":
            verif["pct_canal_vacio"] < 0.02,
    }
    for regla, ok in umbrales.items():
        print(f"  [{'OK ' if ok else 'ALERTA'}] {regla}")
    log["umbrales_contrato"] = umbrales
    (LOGS / "log_calidad_batch.json").write_text(
        json.dumps(log, indent=2, ensure_ascii=False), encoding="utf-8")

    # ------------------------------------------------------------------
    # 4. GOLD: modelo estrella
    # ------------------------------------------------------------------
    titulo("4. Construcción de la capa GOLD (modelo estrella)")
    silver = spark.read.parquet(str(STAGING / "ventas_boletas"))
    catalogo = (spark.read.option("header", True).schema(ESQUEMA_CATALOGO)
                .csv(str(CATALOGO_FUNCIONES)))

    dim_funcion = catalogo.select(
        F.col("id_funcion").alias("sk_funcion"), "obra", "sala", "aforo",
        F.to_date("fecha_funcion").alias("fecha_funcion"), "hora_funcion")

    dim_fecha = (silver.select("fecha_funcion").distinct()
                 .select(
                     F.date_format("fecha_funcion", "yyyyMMdd")
                      .cast("int").alias("sk_fecha"),
                     F.col("fecha_funcion").alias("fecha"),
                     F.year("fecha_funcion").alias("anio"),
                     F.month("fecha_funcion").alias("mes"),
                     F.dayofmonth("fecha_funcion").alias("dia"),
                     F.date_format("fecha_funcion", "EEEE").alias("dia_semana"),
                     F.dayofweek("fecha_funcion").isin(1, 7)
                      .alias("es_fin_semana")))

    dim_tipo = spark.createDataFrame(
        [(i + 1, t) for i, t in enumerate(TIPOS_ENTRADA)],
        ["sk_tipo_entrada", "tipo_entrada"])
    dim_canal = spark.createDataFrame(
        [(i + 1, c) for i, c in enumerate(CANALES + [CANAL_NO_ESPECIFICADO])],
        ["sk_canal", "canal_venta"])

    hecho = (silver
             .join(F.broadcast(dim_tipo), "tipo_entrada")
             .join(F.broadcast(dim_canal), "canal_venta")
             .select(
                 "id_venta",
                 F.col("id_funcion").alias("sk_funcion"),
                 F.date_format("fecha_funcion", "yyyyMMdd").cast("int")
                  .alias("sk_fecha"),
                 "sk_tipo_entrada", "sk_canal",
                 "precio",
                 F.lit(1).alias("cantidad")))

    for nombre, tabla in [("dim_funcion", dim_funcion),
                          ("dim_fecha", dim_fecha),
                          ("dim_tipo_entrada", dim_tipo),
                          ("dim_canal", dim_canal),
                          ("hecho_venta_boleta", hecho)]:
        tabla.write.mode("overwrite").parquet(str(GOLD / nombre))
        guardar_csv(tabla, nombre)
        print(f"  {nombre:<20} {spark.read.parquet(str(GOLD / nombre)).count():>5} filas")

    # Integridad referencial: toda fila del hecho debe tener su función
    huerfanos = hecho.join(dim_funcion, "sk_funcion", "left_anti").count()
    print(f"  Hechos sin función en dim_funcion: {huerfanos}")

    # ------------------------------------------------------------------
    # 5. Indicadores (preguntas analíticas del proyecto)
    # ------------------------------------------------------------------
    titulo("5. Indicadores de negocio (capa analítica)")
    h = (spark.read.parquet(str(GOLD / "hecho_venta_boleta"))
         .join(spark.read.parquet(str(GOLD / "dim_funcion")), "sk_funcion")
         .join(spark.read.parquet(str(GOLD / "dim_canal")), "sk_canal")
         .join(spark.read.parquet(str(GOLD / "dim_tipo_entrada")),
               "sk_tipo_entrada"))
    h.createOrReplaceTempView("ventas")
    total_ingreso = h.agg(F.sum("precio")).first()[0]

    # P1. Boletas e ingresos por obra
    por_obra = (h.groupBy("obra")
                .agg(F.sum("cantidad").alias("boletas"),
                     F.sum("precio").alias("ingreso_cop"),
                     F.countDistinct("sk_funcion").alias("funciones"))
                .withColumn("ingreso_promedio_funcion",
                            F.round(F.col("ingreso_cop") / F.col("funciones")))
                .withColumn("participacion_ingreso_pct",
                            F.round(F.col("ingreso_cop") / total_ingreso * 100, 1))
                .orderBy(F.desc("ingreso_cop")))

    # P2. Ocupación por función frente al aforo (aforo = supuesto v1)
    ocupacion = (h.groupBy("sk_funcion", "obra", "fecha_funcion",
                           "hora_funcion", "aforo")
                 .agg(F.sum("cantidad").alias("boletas"),
                      F.sum("precio").alias("ingreso_cop"))
                 .withColumn("ocupacion_pct",
                             F.round(F.col("boletas") / F.col("aforo") * 100, 1))
                 .orderBy("sk_funcion"))

    # P3. Canal de venta: volumen e ingreso (con Spark SQL)
    por_canal = spark.sql("""
        SELECT canal_venta,
               COUNT(*)                               AS boletas,
               SUM(precio)                            AS ingreso_cop,
               ROUND(AVG(precio))                     AS precio_promedio,
               ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_boletas
        FROM ventas
        GROUP BY canal_venta
        ORDER BY ingreso_cop DESC
    """)

    # P4. Distribución por tipo de entrada
    por_tipo = (h.groupBy("tipo_entrada")
                .agg(F.count("*").alias("boletas"),
                     F.sum("precio").alias("ingreso_cop"))
                .withColumn("pct_boletas", F.round(
                    F.col("boletas") / F.sum("boletas").over(Window.partitionBy())
                    * 100, 1))
                .orderBy(F.desc("boletas")))

    # P5. Matriz obra x canal (pivot) para decisiones de comercialización
    obra_canal = (h.groupBy("obra").pivot("canal_venta", CANALES +
                                          [CANAL_NO_ESPECIFICADO])
                  .agg(F.count("*")).na.fill(0).orderBy("obra"))

    indicadores = {"ind_por_obra": por_obra, "ind_ocupacion_funcion": ocupacion,
                   "ind_por_canal": por_canal, "ind_por_tipo_entrada": por_tipo,
                   "ind_obra_x_canal": obra_canal}
    for nombre, tabla in indicadores.items():
        print(f"\n--- {nombre} ---")
        tabla.show(truncate=False)
        tabla.write.mode("overwrite").parquet(str(GOLD / nombre))
        guardar_csv(tabla, nombre)

    print(f"Ingreso total validado: ${total_ingreso:,.0f} COP")

    # Evidencia del plan físico (optimizador Catalyst)
    plan = por_obra._jdf.queryExecution().explainString(
        spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString(
            "formatted"))
    (LOGS / "plan_ejecucion_por_obra.txt").write_text(plan, encoding="utf-8")
    print("\nPlan físico guardado en evidencia/logs_calidad/"
          "plan_ejecucion_por_obra.txt")

    df.unpersist()
    print(f"\nPipeline completado en {time.time() - inicio:.1f} s")
    spark.stop()


if __name__ == "__main__":
    main()
