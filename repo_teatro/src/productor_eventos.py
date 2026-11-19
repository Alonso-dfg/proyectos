"""
Productor de eventos de venta de boletas (simulador de la taquilla y la web)
============================================================================

Convierte cada fila del archivo RAW en un EVENTO JSON "venta_boleta" tal como
lo emitiría el punto de venta en el momento de vender la boleta, y lo publica:

  * --destino archivos (por defecto): escribe micro-lotes JSON Lines en
    data/stream/entrada/. Spark Structured Streaming observa esa carpeta.
  * --destino kafka: publica cada evento en el tópico `ventas_boletas` de un
    broker Apache Kafka (ver docker-compose.yml). La llave del mensaje es el
    id_funcion, así todas las ventas de una función caen en la MISMA
    partición y Kafka garantiza su orden.

Qué simula (para que el streaming enfrente los problemas reales de la clase)
-------------------------------------------------------------------------
* Hora del evento (`evento_ts`): momento de la venta, entre 14 días antes de
  la función y una hora antes de que empiece.
* Hora de llegada: el evento llega al sistema un poco después de ocurrir. La
  diferencia entre ambas es la razón de usar *event time* y no la hora de
  llegada.
* Eventos tardíos: ~5 % llega entre 2 y 12 horas tarde (el watermark de 1 día
  los acepta) y 6 eventos llegan 4-6 días tarde (el watermark los descarta;
  el batch nocturno los recupera en la conciliación).
* Duplicados: los 8 id_venta repetidos del RAW se re-envían minutos después,
  como un reintento del POS. El consumidor debe contarlos una sola vez.
* Datos sucios: obra con espacios/mayúsculas, fechas DD/MM/AAAA, canal vacío
  y precios 0 viajan tal cual; el contrato se aplica en el consumidor.

Ejecución
---------
    # Modo en vivo (un lote = un día de ventas, cada 1,5 s)
    python3 src/productor_eventos.py --limpiar --pausa 1.5
    # Modo Kafka (requiere `docker compose up -d` y `pip install kafka-python-ng`)
    python3 src/productor_eventos.py --destino kafka --pausa 1.5
"""
import argparse
import csv
import json
import random
import shutil
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from comun import CATALOGO_FUNCIONES, RAW_CSV, STREAM

ENTRADA = STREAM / "entrada"
CONTROL = STREAM / "control"
TOPICO = "ventas_boletas"
SEMILLA = 2026
N_MUY_TARDIOS = 6


def leer_catalogo():
    with open(CATALOGO_FUNCIONES, encoding="utf-8") as f:
        return {int(r["id_funcion"]): r for r in csv.DictReader(f)}


def construir_eventos():
    """Devuelve la lista de eventos con su hora de evento y de llegada."""
    rnd = random.Random(SEMILLA)
    catalogo = leer_catalogo()
    with open(RAW_CSV, encoding="utf-8") as f:
        filas = list(csv.DictReader(f))

    vistos = {}
    eventos = []
    for fila in filas:
        funcion = catalogo[int(fila["id_funcion"])]
        inicio = datetime.fromisoformat(
            f"{funcion['fecha_funcion']}T{funcion['hora_funcion']}")
        if fila["id_venta"] in vistos:
            # Reintento del POS: mismo evento (misma hora de venta), llega
            # entre 10 y 90 minutos después del original.
            original = vistos[fila["id_venta"]]
            evento_ts = original["evento_ts"]
            llegada = original["llegada"] + timedelta(minutes=rnd.randint(10, 90))
            tipo_llegada = "reintento_duplicado"
        else:
            apertura = inicio - timedelta(days=14)
            cierre = inicio - timedelta(hours=1)
            seg = rnd.uniform(0, (cierre - apertura).total_seconds())
            evento_ts = (apertura + timedelta(seconds=seg)).replace(microsecond=0)
            llegada = evento_ts + timedelta(seconds=rnd.randint(1, 300))
            tipo_llegada = "normal"
            if rnd.random() < 0.05:
                llegada = evento_ts + timedelta(hours=rnd.uniform(2, 12))
                tipo_llegada = "tardio_tolerado"
        ev = {**fila, "evento_ts": evento_ts, "llegada": llegada,
              "tipo_llegada": tipo_llegada}
        vistos.setdefault(fila["id_venta"], ev)
        eventos.append(ev)

    # Eventos MUY tardíos: ventas válidas que se quedaron "atascadas" en un
    # equipo sin conexión y llegan días después.
    candidatos = [e for e in eventos if e["tipo_llegada"] == "normal"
                  and e["tipo_entrada"] == "General" and e["precio"] == "25000"]
    for ev in rnd.sample(candidatos, N_MUY_TARDIOS):
        ev["llegada"] = ev["evento_ts"] + timedelta(days=rnd.uniform(4, 6))
        ev["tipo_llegada"] = "muy_tardio"

    eventos.sort(key=lambda e: e["llegada"])
    return eventos


def a_json(ev, producido_ts):
    return {
        "id_venta": ev["id_venta"], "id_funcion": ev["id_funcion"],
        "obra": ev["obra"], "fecha_funcion": ev["fecha_funcion"],
        "hora_funcion": ev["hora_funcion"], "tipo_entrada": ev["tipo_entrada"],
        "canal_venta": ev["canal_venta"], "precio": ev["precio"],
        "evento_ts": ev["evento_ts"].isoformat(),
        "producido_ts": producido_ts,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--destino", choices=["archivos", "kafka"],
                    default="archivos")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--pausa", type=float, default=1.5,
                    help="segundos entre lotes (0 = todo de una vez)")
    ap.add_argument("--limpiar", action="store_true",
                    help="borra data/stream antes de empezar")
    args = ap.parse_args()

    if args.limpiar and STREAM.exists():
        shutil.rmtree(STREAM)
    ENTRADA.mkdir(parents=True, exist_ok=True)
    CONTROL.mkdir(parents=True, exist_ok=True)
    (CONTROL / "FIN").unlink(missing_ok=True)

    eventos = construir_eventos()
    # Un lote = todas las llegadas de un mismo día (un "día de ventas").
    lotes = defaultdict(list)
    for ev in eventos:
        lotes[ev["llegada"].date()].append(ev)

    productor = None
    if args.destino == "kafka":
        from kafka import KafkaProducer  # pip install kafka-python-ng
        productor = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            key_serializer=lambda k: str(k).encode(),
            value_serializer=lambda v: json.dumps(v).encode(),
            acks="all")

    print(f"Publicando {len(eventos)} eventos en {len(lotes)} lotes "
          f"-> {args.destino}")
    for n, (dia, lote) in enumerate(sorted(lotes.items()), start=1):
        producido = datetime.now().isoformat(timespec="milliseconds")
        registros = [a_json(ev, producido) for ev in lote]
        if productor:
            for r in registros:
                productor.send(TOPICO, key=r["id_funcion"], value=r)
            productor.flush()
        else:
            # Escritura atómica: se escribe un archivo oculto y se renombra.
            # Spark ignora archivos que empiezan por "." y nunca lee uno a medias.
            destino = ENTRADA / f"lote_{n:03d}_{dia.isoformat()}.json"
            tmp = ENTRADA / f".{destino.name}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                for r in registros:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
            tmp.rename(destino)
        tipos = Counter(ev["tipo_llegada"] for ev in lote)
        print(f"  lote {n:03d} | día de llegada {dia} | {len(lote):3d} eventos"
              f" | {dict(tipos)}")
        if args.pausa:
            time.sleep(args.pausa)

    manifiesto = {
        "destino": args.destino,
        "eventos_publicados": len(eventos),
        "lotes": len(lotes),
        "por_tipo_llegada": dict(Counter(e["tipo_llegada"] for e in eventos)),
        "ids_muy_tardios": sorted(int(e["id_venta"]) for e in eventos
                                  if e["tipo_llegada"] == "muy_tardio"),
        "ids_duplicados": sorted({int(e["id_venta"]) for e in eventos
                                  if e["tipo_llegada"] == "reintento_duplicado"}),
        "rango_evento_ts": [min(e["evento_ts"] for e in eventos).isoformat(),
                            max(e["evento_ts"] for e in eventos).isoformat()],
    }
    (CONTROL / "manifiesto_productor.json").write_text(
        json.dumps(manifiesto, indent=2, ensure_ascii=False), encoding="utf-8")
    (CONTROL / "FIN").write_text(datetime.now().isoformat(), encoding="utf-8")
    if productor:
        productor.close()
    print(json.dumps(manifiesto, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
