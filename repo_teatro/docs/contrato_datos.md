# Contrato de datos — venta_boleta

Acuerdo entre quien produce el dato (taquilla / plataforma en línea) y quien
lo consume (equipo de datos / dirección del teatro). A diferencia del
diccionario de datos, este documento define reglas verificables de calidad
y la acción a tomar cuando una regla se incumple.

| Campo | Tipo | Formato | Regla | Ejemplo | Acción ante error |
|---|---|---|---|---|---|
| id_venta | Entero | Numérico | Único, no nulo, llave primaria de la venta | 1000234 | Descartar el registro y reportarlo en el log de calidad |
| fecha_funcion | Fecha | YYYY-MM-DD | No nula; no puede ser anterior a 2024-01-01 | 2026-08-15 | Marcar como "pendiente de revisión" y excluir de reportes de ingreso |
| obra | Texto | Cadena normalizada (sin espacios extra, sin mayúsculas forzadas) | No nulo; debe existir en el catálogo de obras del teatro | La Casa de Bernarda Alba | Enviar a revisión manual si no coincide con el catálogo |
| tipo_entrada | Categórico | Uno de: General, Preferencial, Cortesia | Debe pertenecer al conjunto permitido | General | Reclasificar como "Sin clasificar" y reportar |
| precio | Decimal (COP) | Numérico >= 0 | Si tipo_entrada = Cortesia, precio debe ser 0 | 25000.00 | Marcar para corrección manual antes de pasar a STAGING |
| canal_venta | Categórico | Uno de: Taquilla, En linea, Convenio | No nulo | En linea | Imputar "No especificado" y reportar en el diagnóstico de completitud |

## Umbrales de calidad exigidos antes de promover un lote de RAW a STAGING

- **Completitud**: `canal_venta` con menos del 2% de valores vacíos (el
  perfilamiento inicial mostró 5.0%; pendiente de mejora).
- **Unicidad**: 0% de valores duplicados en `id_venta` (el perfilamiento
  inicial encontró 8 valores duplicados; pendiente de corrección).
- **Validez**: 100% de `fecha_funcion` en formato YYYY-MM-DD antes de cargar
  a STAGING (el perfilamiento inicial encontró 45 registros en formato
  DD/MM/AAAA).
- **Consistencia**: 0 registros con `tipo_entrada` distinto de "Cortesia" y
  `precio` = 0 (el perfilamiento inicial encontró 20 casos).

## Contrato del evento en tiempo real (v3)

Cuando la venta llega como evento (archivo JSON o tópico Kafka
`ventas_boletas`), se aplican **las mismas reglas de campo** de la tabla
anterior (código único en `src/contrato.py`) más estas reglas propias del
streaming:

| Campo / regla | Tipo | Regla | Acción ante error |
|---|---|---|---|
| evento_ts | Timestamp | Momento de la venta (event time). No nulo. | Sin evento_ts el registro no entra a ventanas ni deduplicación; se revisa en el batch. |
| producido_ts | Timestamp | Momento en que el POS/web emitió el evento. | Solo se usa para medir latencia. |
| Llave del mensaje Kafka | Texto | `id_funcion` (todas las ventas de una función en la misma partición). | — |
| Esquema | JSON | Esquema explícito; no se infiere. | Campos ausentes llegan como null y caen en cuarentena. |
| Deduplicación | — | Un `id_venta` se cuenta una sola vez dentro del watermark (1 día). | Reintentos del POS se descartan. |
| Tardanza máxima | — | Watermark de 1 día sobre `evento_ts`. | Eventos más tardíos se descartan del streaming y los recupera la conciliación con el batch nocturno. |

## Umbrales del streaming

- Latencia p95 (emisión → procesamiento) menor a 5 s. Medido: 1,9 s.
- 0 `id_venta` duplicados en la capa silver del streaming. Medido: 0.
- Diferencia streaming vs. batch explicada al 100 % por eventos tardíos.
  Medido: 5 de 5 explicados.

## Versión y cambios

- v1 (semana 3): definición inicial del contrato, basada en el
  perfilamiento del primer archivo RAW recibido.
- v2 (procesamiento distribuido): las reglas se implementan en PySpark; los
  registros que fallan van a `data/quarantine/` en lugar de borrarse; se
  agrega linaje (`_archivo_origen`, `_fecha_proceso`, `_version_contrato`).
- v3 (streaming): reglas movidas a `src/contrato.py` para que batch y
  streaming usen el mismo código; se agrega el contrato del evento
  (`evento_ts`, `producido_ts`), watermark y deduplicación por `id_venta`;
  nueva regla `precio_nulo_o_negativo`.
