# Mapa del viaje del dato — venta_boleta

| Etapa | Descripción | Responsable | Riesgos identificados |
|---|---|---|---|
| FUENTE | Taquilla física (registro manual/POS) y plataforma de venta en línea del teatro. | Encargado de taquilla / administrador de la plataforma en línea | Registro inconsistente de canal o tipo de entrada en el punto de venta. |
| RAW | Exportación diaria sin transformar a `ventas_boletas_raw.csv`, con todos los campos tal como se capturaron. | Integrante encargado de ingesta (equipo) | Duplicados por reintentos de exportación; ausencia de control de versiones del archivo. |
| STAGING / SILVER | Limpieza de duplicados, normalización de fechas y texto, validación de reglas del contrato de datos. | Integrante encargado de calidad de datos (equipo) | Pérdida de trazabilidad si no se conserva el `id_venta` original tras la limpieza. |
| Consumidor / capa analítica | Modelo de hechos y dimensiones para reportes de ingresos, ocupación y canal de venta. | Dirección administrativa y programación artística del teatro | Decisiones basadas en datos no depurados si se consulta directamente el RAW. |

## Ruta en tiempo real (streaming)

| Etapa | Descripción | Responsable | Riesgos identificados |
|---|---|---|---|
| FUENTE (evento) | POS de taquilla y web emiten un evento JSON por boleta vendida. | Administrador de la plataforma / taquilla | Reintentos que duplican eventos; equipos sin conexión que envían tarde. |
| BRONZE en vivo | Tópico Kafka `ventas_boletas` (3 particiones, retención 7 días) o carpeta `data/stream/entrada/`. | Equipo de datos (ingesta) | Pérdida de mensajes si la retención vence antes de procesarlos. |
| SILVER en vivo | Spark Structured Streaming aplica el contrato, watermark de 1 día y deduplicación; separa cuarentena. | Equipo de datos (calidad) | Eventos más tardíos que el watermark quedan fuera del tiempo real. |
| GOLD en vivo | Ocupación por función, ventas por día y canal, alertas ≥ 90 %. | Dirección / taquilla (consumo) | Decidir con una cifra operativa que aún no está conciliada. |
| Conciliación | El batch nocturno compara contra el streaming y recupera los eventos tardíos; la cifra oficial es la del batch. | Equipo de datos | Diferencias no explicadas deben escalarse. |

## Trazabilidad mínima a conservar en cada etapa

- Sistema y momento de origen del dato.
- Fecha y responsable de cada transformación.
- Identificador original del registro (`id_venta`).
- Versión del esquema/contrato aplicado en cada carga.
- En streaming: `_origen` (tópico/partición/offset o archivo), `_batch_id` y checkpoints por consulta.
