"""
Consumidor de streaming: Spark Structured Streaming sobre eventos de venta
==========================================================================

Proyecto integrador · Gestión de venta de boletas de un teatro
Ingeniería de Datos · Streaming, Apache Kafka y Spark Structured Streaming

Arquitectura medallion en tiempo real
-------------------------------------
    Productor (taquilla / web)            src/productor_eventos.py
        │  eventos JSON "venta_boleta"
        ▼
    BRONZE  carpeta data/stream/entrada/   o   tópico Kafka `ventas_boletas`
        │  readStream + esquema explícito
        ▼
    Contrato de datos (src/contrato.py)  ← las MISMAS reglas que el batch
    + watermark de 1 día sobre evento_ts + deduplicación por id_venta
        │
        ├─► Consulta 1  SILVER       (append, foreachBatch idempotente)
        │      silver/ + cuarentena/ + métricas de latencia por micro-lote
        ├─► Consulta 2  GOLD ocupación por función (complete) + ALERTAS ≥ 90 %
        └─► Consulta 3  GOLD ventas por día y canal (ventana de 1 día, update)

Por qué así
-----------
* Event time vs. hora de llegada: los indicadores se calculan con `evento_ts`
  (cuándo se vendió la boleta), no con la hora en que el evento llegó.
* Watermark: le dice a Spark cuánto esperar eventos tardíos (1 día). Sin él,
  el estado de la deduplicación y de las ventanas crecería sin límite.
* Deduplicación: el POS reintenta envíos; `dropDuplicatesWithinWatermark`
  cuenta cada id_venta una sola vez mientras siga dentro del watermark.
* Checkpoints: cada consulta guarda offsets y estado en
  data/stream/_checkpoints/<consulta>. Si el proceso se cae, al reiniciarlo
  continúa donde iba, sin perder ni repetir micro-lotes.
* Idempotencia: la capa silver se escribe particionada por `_batch_id` con
  sobrescritura dinámica; reprocesar un micro-lote reemplaza su partición en
  vez de duplicar filas.

Ejecución
---------
    # Terminal 1 (consumidor)       # Terminal 2 (productor)
    python3 src/streaming_ventas.py   python3 src/productor_eventos.py --pausa 1.5

    # Con Kafka (docker compose up -d):
    python3 src/streaming_ventas.py --fuente kafka
    python3 src/productor_eventos.py --destino kafka

    # Reprocesar todo lo que ya está en la carpeta y terminar:
    python3 src/streaming_ventas.py --disponible-ahora
"""
import argparse
import json
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from comun import (CATALOGO_FUNCIONES, RAIZ, STAGING, STREAM,
                   UMBRAL_ALERTA_OCUPACION, crear_spark)
from contrato import VERSION_CONTRATO, aplicar_contrato, es_valido

ENTRADA = STREAM / "entrada"
CONTROL = STREAM / "control"
SILVER = STREAM / "silver"
CUARENTENA = STREAM / "cuarentena"
GOLD = STREAM / "gold"
CHECKPOINTS = STREAM / "_checkpoints"
LOGS = RAIZ / "evidencia" / "logs_streaming"
TOPICO = "ventas_boletas"

# Esquema explícito del evento: en streaming NO se infiere el esquema (la
# fuente puede estar vacía al arrancar y un archivo raro cambiaría los tipos).
ESQUEMA_EVENTO = StructType([
    StructField("id_venta", StringType(), True),
    StructField("id_funcion", StringType(), True),
    StructField("obra", StringType(), True),
    StructField("fecha_funcion", StringType(), True),
    StructField("hora_funcion", StringType(), True),
    StructField("tipo_entrada", StringType(), True),
    StructField("canal_venta", StringType(), True),
    StructField("precio", StringType(), True),
    StructField("evento_ts", TimestampType(), True),
    StructField("producido_ts", TimestampType(), True),
])

COLUMNAS_SILVER = ["id_venta", "id_funcion", "obra", "fecha_funcion",
                   "hora_funcion", "tipo_entrada", "canal_venta", "precio",
                   "evento_ts", "producido_ts", "_ingesta_ts", "latencia_s",
                   "obra_original", "fecha_original", "fecha_corregida",
                   "canal_imputado", "_origen"]


# ---------------------------------------------------------------------------
# Fuente (BRONZE)
# ---------------------------------------------------------------------------
def leer_fuente(spark, args):
    if args.fuente == "kafka":
        crudo = (spark.readStream.format("kafka")
                 .option("kafka.bootstrap.servers", args.bootstrap)
                 .option("subscribe", TOPICO)
                 .option("startingOffsets", "earliest")
                 .option("failOnDataLoss", "false")
                 .load())
        # value llega en bytes: se convierte a texto y se interpreta el JSON.
        return (crudo
                .select(F.from_json(F.col("value").cast("string"),
                                    ESQUEMA_EVENTO).alias("e"),
                        "partition", "offset")
                .select("e.*", F.concat(F.lit(f"kafka:{TOPICO}/p"),
                                        F.col("partition"), F.lit("@"),
                                        F.col("offset")).alias("_origen")))
    ENTRADA.mkdir(parents=True, exist_ok=True)
    return (spark.readStream
            .schema(ESQUEMA_EVENTO)
            # 1 archivo (= 1 día de ventas) por micro-lote: hace visible
            # cómo avanza el watermark lote a lote.
            .option("maxFilesPerTrigger", args.max_archivos)
            .json(str(ENTRADA))
            .withColumn("_origen", F.element_at(
                F.split(F.input_file_name(), "/"), -1)))


def flujo_limpio(spark, args):
    """BRONZE -> contrato -> watermark -> deduplicación (transformaciones)."""
    return (aplicar_contrato(leer_fuente(spark, args))
            .withColumn("_ingesta_ts", F.current_timestamp())
            .withWatermark("evento_ts", args.watermark)
            .dropDuplicatesWithinWatermark(["id_venta"])
            # Latencia: desde que el productor emitió el evento hasta que
            # Spark lo procesa (segundos, con milisegundos).
            .withColumn("latencia_s", F.round(
                F.col("_ingesta_ts").cast("double")
                - F.col("producido_ts").cast("double"), 3)))


# ---------------------------------------------------------------------------
# Consulta 1: SILVER + CUARENTENA + métricas por micro-lote
# ---------------------------------------------------------------------------
METRICAS = []


def procesar_silver(lote, batch_id):
    lote = lote.persist()
    marcado = lote.withColumn("_batch_id", F.lit(batch_id))
    (marcado.filter(es_valido()).select(*COLUMNAS_SILVER, "_batch_id")
     .write.mode("overwrite").partitionBy("_batch_id").parquet(str(SILVER)))
    (marcado.filter(~es_valido())
     .select(*COLUMNAS_SILVER,
             F.concat_ws("|", "motivos_cuarentena").alias("motivos_cuarentena"),
             "_batch_id")
     .write.mode("overwrite").partitionBy("_batch_id").parquet(str(CUARENTENA)))

    r = lote.agg(
        F.count("*").alias("eventos"),
        F.sum(es_valido().cast("int")).alias("validos"),
        F.round(F.avg("latencia_s"), 3).alias("latencia_prom_s"),
        F.max("latencia_s").alias("latencia_max_s"),
        F.min("evento_ts").alias("evento_min"),
        F.max("evento_ts").alias("evento_max")).first()
    lote.unpersist()
    if not r["eventos"]:
        return
    fila = {"batch_id": batch_id, "eventos": r["eventos"],
            "validos": r["validos"], "cuarentena": r["eventos"] - r["validos"],
            "latencia_prom_s": r["latencia_prom_s"],
            "latencia_max_s": r["latencia_max_s"],
            "evento_min": str(r["evento_min"]), "evento_max": str(r["evento_max"])}
    METRICAS.append(fila)
    print(f"[silver] lote {batch_id:>3} | {fila['eventos']:>3} eventos | "
          f"{fila['validos']:>3} válidos | {fila['cuarentena']} cuarentena | "
          f"latencia prom {fila['latencia_prom_s']} s")


# ---------------------------------------------------------------------------
# Consulta 2: GOLD ocupación por función + alertas operativas
# ---------------------------------------------------------------------------
AFORO = pd.read_csv(CATALOGO_FUNCIONES)
ALERTADAS = set()
HISTORIAL_OCUPACION = []


def procesar_ocupacion(lote, batch_id):
    # outputMode("complete"): cada micro-lote trae la tabla de resultados
    # COMPLETA (máx. 15 filas, una por función), por eso es seguro toPandas().
    pdf = lote.toPandas()
    if pdf.empty:
        return
    pdf = (AFORO[["id_funcion", "obra", "fecha_funcion", "hora_funcion", "aforo"]]
           .merge(pdf, on="id_funcion", how="left").fillna(
               {"boletas": 0, "ingreso_cop": 0}))
    pdf["boletas"] = pdf["boletas"].astype(int)
    pdf["ingreso_cop"] = pdf["ingreso_cop"].astype("int64")
    pdf["ocupacion_pct"] = (pdf["boletas"] / pdf["aforo"] * 100).round(1)
    pdf["estado"] = pd.cut(pdf["ocupacion_pct"], [-1, 50, 89.99, 1000],
                           labels=["Baja", "Media", "Alta (alerta)"])
    pdf["actualizado"] = datetime.now().isoformat(timespec="seconds")
    GOLD.mkdir(parents=True, exist_ok=True)
    pdf.drop(columns=["ultima_venta"]).to_csv(
        GOLD / "ocupacion_funcion.csv", index=False)
    (pdf.groupby("obra", as_index=False)[["boletas", "ingreso_cop"]].sum()
     .sort_values("ingreso_cop", ascending=False)
     .to_csv(GOLD / "ingresos_por_obra.csv", index=False))
    for _, f in pdf.iterrows():
        HISTORIAL_OCUPACION.append({"batch_id": batch_id,
                                    "id_funcion": int(f["id_funcion"]),
                                    "boletas": int(f["boletas"]),
                                    "ocupacion_pct": float(f["ocupacion_pct"])})

    nuevas = pdf[(pdf["ocupacion_pct"] >= UMBRAL_ALERTA_OCUPACION * 100)
                 & (~pdf["id_funcion"].isin(ALERTADAS))]
    with open(GOLD / "alertas.jsonl", "a", encoding="utf-8") as f:
        for _, a in nuevas.iterrows():
            ALERTADAS.add(int(a["id_funcion"]))
            alerta = {"batch_id": batch_id,
                      "detectada": datetime.now().isoformat(timespec="seconds"),
                      "id_funcion": int(a["id_funcion"]), "obra": a["obra"],
                      "fecha_funcion": a["fecha_funcion"],
                      "boletas": int(a["boletas"]), "aforo": int(a["aforo"]),
                      "ocupacion_pct": float(a["ocupacion_pct"]),
                      "ultima_venta": str(a["ultima_venta"]),
                      "dias_antes_de_la_funcion": round(
                          (pd.Timestamp(f"{a['fecha_funcion']} {a['hora_funcion']}")
                           - pd.Timestamp(a["ultima_venta"])).total_seconds()
                          / 86400, 1),
                      "accion_sugerida": "Abrir lista de espera / evaluar "
                                         "función adicional; frenar cortesías"}
            f.write(json.dumps(alerta, ensure_ascii=False) + "\n")
            print(f"[ALERTA] Función {alerta['id_funcion']} ({alerta['obra']}, "
                  f"{alerta['fecha_funcion']}) al {alerta['ocupacion_pct']} % "
                  f"de ocupación")


# ---------------------------------------------------------------------------
# Consulta 3: GOLD ventas por ventana diaria y canal
# ---------------------------------------------------------------------------
VENTANAS = {}


def procesar_ventanas(lote, batch_id):
    # outputMode("update"): solo llegan las ventanas que cambiaron en este
    # micro-lote; se hace "upsert" sobre la tabla acumulada.
    for r in lote.collect():
        clave = (str(r["inicio_ventana"]), r["canal_venta"])
        VENTANAS[clave] = {"inicio_ventana": clave[0], "canal_venta": clave[1],
                           "boletas": r["boletas"], "ingreso_cop": r["ingreso_cop"],
                           "batch_id": batch_id}
    if VENTANAS:
        GOLD.mkdir(parents=True, exist_ok=True)
        (pd.DataFrame(VENTANAS.values())
         .sort_values(["inicio_ventana", "canal_venta"])
         .to_csv(GOLD / "ventas_dia_canal.csv", index=False))


# ---------------------------------------------------------------------------
# Conciliación con el batch (control de calidad del streaming)
# ---------------------------------------------------------------------------
def conciliar(spark):
    resultado = {}
    silver = spark.read.parquet(str(SILVER))
    resultado["boletas_streaming"] = silver.count()
    resultado["ingreso_streaming_cop"] = silver.agg(F.sum("precio")).first()[0]
    resultado["cuarentena_streaming"] = (spark.read.parquet(str(CUARENTENA))
                                         .count() if CUARENTENA.exists() else 0)
    lat = silver.agg(
        F.round(F.avg("latencia_s"), 3).alias("prom"),
        F.round(F.percentile_approx("latencia_s", 0.5), 3).alias("p50"),
        F.round(F.percentile_approx("latencia_s", 0.95), 3).alias("p95"),
        F.round(F.max("latencia_s"), 3).alias("max")).first().asDict()
    resultado["latencia_s"] = lat

    manifiesto_p = CONTROL / "manifiesto_productor.json"
    manifiesto = (json.loads(manifiesto_p.read_text(encoding="utf-8"))
                  if manifiesto_p.exists() else {})
    resultado["eventos_publicados"] = manifiesto.get("eventos_publicados")

    staging_p = STAGING / "ventas_boletas"
    if staging_p.exists():
        batch = spark.read.parquet(str(staging_p))
        resultado["boletas_batch"] = batch.count()
        resultado["ingreso_batch_cop"] = batch.agg(F.sum("precio")).first()[0]
        solo_batch = sorted(r[0] for r in batch.select("id_venta")
                            .subtract(silver.select("id_venta")).collect())
        solo_stream = sorted(r[0] for r in silver.select("id_venta")
                             .subtract(batch.select("id_venta")).collect())
        resultado["ids_solo_en_batch"] = solo_batch
        resultado["ids_solo_en_streaming"] = solo_stream
        resultado["faltantes_explicados_por_eventos_muy_tardios"] = (
            set(solo_batch) <= set(manifiesto.get("ids_muy_tardios", [])))
        resultado["duplicados_en_streaming"] = (
            resultado["boletas_streaming"]
            - silver.select("id_venta").distinct().count())
        recuperar = batch.filter(F.col("id_venta").isin(solo_batch))
        resultado["ingreso_a_recuperar_en_batch_cop"] = (
            recuperar.agg(F.sum("precio")).first()[0] or 0)
    return resultado


# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fuente", choices=["archivos", "kafka"], default="archivos")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--trigger", type=int, default=2,
                    help="intervalo del micro-lote en segundos")
    ap.add_argument("--watermark", default="1 day")
    ap.add_argument("--max-archivos", type=int, default=1)
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--disponible-ahora", action="store_true",
                    help="procesa lo disponible y termina (trigger availableNow)")
    ap.add_argument("--reiniciar", action="store_true",
                    help="borra checkpoints y salidas del streaming")
    args = ap.parse_args()

    if args.reiniciar:
        for p in (SILVER, CUARENTENA, GOLD, CHECKPOINTS):
            shutil.rmtree(p, ignore_errors=True)

    paquetes = None
    if args.fuente == "kafka":
        import pyspark
        paquetes = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"
    spark = crear_spark("TeatroStreaming", paquetes)
    # Sobrescribir solo la partición del micro-lote que se está escribiendo.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    limpio = flujo_limpio(spark, args)
    validos = limpio.filter(es_valido())

    ocupacion = (validos.groupBy("id_funcion")
                 .agg(F.count("*").alias("boletas"),
                      F.sum("precio").alias("ingreso_cop"),
                      F.max("evento_ts").alias("ultima_venta")))

    ventanas = (validos
                # startTime="5 hours": las ventanas se alinean a la medianoche
                # de Bogotá (UTC-5) y no a la medianoche UTC.
                .groupBy(F.window("evento_ts", "1 day", startTime="5 hours")
                         .alias("ventana"),
                         "canal_venta")
                .agg(F.count("*").alias("boletas"),
                     F.sum("precio").alias("ingreso_cop"))
                .select(F.col("ventana.start").alias("inicio_ventana"),
                        "canal_venta", "boletas", "ingreso_cop"))

    def iniciar(df, nombre, modo, funcion):
        escritor = (df.writeStream.queryName(nombre).outputMode(modo)
                    .foreachBatch(funcion)
                    .option("checkpointLocation", str(CHECKPOINTS / nombre)))
        escritor = (escritor.trigger(availableNow=True) if args.disponible_ahora
                    else escritor.trigger(processingTime=f"{args.trigger} seconds"))
        return escritor.start()

    print(f"Streaming iniciado · fuente={args.fuente} · watermark={args.watermark}"
          f" · contrato {VERSION_CONTRATO}")
    inicio = time.time()
    consultas = [iniciar(limpio, "silver", "append", procesar_silver),
                 iniciar(ocupacion, "gold_ocupacion", "complete",
                         procesar_ocupacion),
                 iniciar(ventanas, "gold_ventanas", "update", procesar_ventanas)]

    if args.disponible_ahora:
        for q in consultas:
            q.awaitTermination()
    else:
        fin = CONTROL / "FIN"
        while time.time() - inicio < args.timeout:
            for q in consultas:
                if q.exception():
                    raise RuntimeError(f"{q.name}: {q.exception()}")
            if fin.exists() and fin.stat().st_mtime > inicio:
                # El productor terminó: se procesa lo pendiente y se cierra.
                for q in consultas:
                    q.processAllAvailable()
                break
            time.sleep(1)

    progreso = {}
    for q in consultas:
        recientes = [p for p in q.recentProgress if p.get("numInputRows")]
        progreso[q.name] = {
            "microlotes_con_datos": len(recientes),
            "filas_leidas": sum(p["numInputRows"] for p in recientes),
            "filas_por_segundo_procesadas_prom": round(
                sum(p.get("processedRowsPerSecond", 0) for p in recientes)
                / max(len(recientes), 1), 1),
            "duracion_trigger_ms_prom": round(
                sum(p["durationMs"].get("triggerExecution", 0) for p in recientes)
                / max(len(recientes), 1)),
            "watermark_final": (q.lastProgress or {}).get("eventTime", {})
                                                     .get("watermark"),
            "filas_estado_final": sum(
                o.get("numRowsTotal", 0)
                for o in (q.lastProgress or {}).get("stateOperators", [])),
            "filas_descartadas_por_watermark": sum(
                o.get("numRowsDroppedByWatermark", 0)
                for p in recientes for o in p.get("stateOperators", [])),
        }
        q.stop()

    LOGS.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(METRICAS).to_csv(LOGS / "metricas_microlotes.csv", index=False)
    pd.DataFrame(HISTORIAL_OCUPACION).to_csv(
        LOGS / "historial_ocupacion.csv", index=False)
    resumen = {"fecha_ejecucion": datetime.now().isoformat(timespec="seconds"),
               "fuente": args.fuente, "watermark": args.watermark,
               "trigger_s": None if args.disponible_ahora else args.trigger,
               "version_contrato": VERSION_CONTRATO,
               "duracion_total_s": round(time.time() - inicio, 1),
               "consultas": progreso,
               "conciliacion_con_batch": conciliar(spark)}
    (LOGS / "resumen_streaming.json").write_text(
        json.dumps(resumen, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8")
    print(json.dumps(resumen, indent=2, ensure_ascii=False, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
