#!/usr/bin/env bash
# Demostración completa del streaming con la fuente de archivos.
# Uso:  bash demo_streaming.sh            (fuente archivos)
#       bash demo_streaming.sh kafka      (requiere: docker compose up -d)
set -euo pipefail
cd "$(dirname "$0")"
FUENTE="${1:-archivos}"

[ -d data/staging/ventas_boletas ] || python3 src/pipeline_batch_spark.py
rm -rf data/stream
python3 src/streaming_ventas.py --fuente "$FUENTE" --reiniciar &
CONSUMIDOR=$!
sleep 20   # tiempo para que Spark arranque las 3 consultas
python3 src/productor_eventos.py --destino "$FUENTE" --pausa 3
wait $CONSUMIDOR
python3 src/graficas.py
echo "Listo: evidencia/logs_streaming/ y evidencia/figuras/"
