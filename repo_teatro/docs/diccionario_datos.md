# Diccionario de datos — venta_boleta

Catálogo descriptivo de cada campo del archivo `data/raw/ventas_boletas_raw.csv`.
A diferencia del contrato de datos (`contrato_datos.md`), este documento solo
describe el significado de cada campo; no impone reglas de cumplimiento.

| Campo | Tipo de dato | Significado de negocio | Ejemplo |
|---|---|---|---|
| id_venta | Entero | Identificador único de la transacción de venta de una boleta. | 1000234 |
| id_funcion | Entero | Identificador de la función (presentación específica de una obra en fecha y hora). | 12 |
| obra | Texto | Nombre de la obra teatral presentada. | La Casa de Bernarda Alba |
| fecha_funcion | Fecha | Fecha en la que se presenta la función. | 2026-08-15 |
| hora_funcion | Hora | Hora de inicio de la función. | 20:00 |
| tipo_entrada | Categórico | Categoría de la boleta vendida (General, Preferencial o Cortesía). | General |
| canal_venta | Categórico | Canal por el cual se realizó la venta (Taquilla, En línea o Convenio). | En linea |
| precio | Decimal (COP) | Valor pagado por la boleta en pesos colombianos. | 25000 |

## Campos adicionales del evento en tiempo real

| Campo | Tipo de dato | Significado de negocio | Ejemplo |
|---|---|---|---|
| evento_ts | Timestamp | Momento en que se vendió la boleta (hora del evento). | 2026-07-05T14:22:10 |
| producido_ts | Timestamp | Momento en que el punto de venta emitió el mensaje. | 2026-11-04T21:16:26.120 |
| _ingesta_ts | Timestamp | Momento en que Spark procesó el evento (lo agrega el consumidor). | 2026-11-04T21:16:27.010 |
| latencia_s | Decimal | `_ingesta_ts - producido_ts`, en segundos. | 0.89 |
| _origen | Texto | Archivo o `tópico/partición@offset` de donde salió el evento. | kafka:ventas_boletas/p1@532 |
| _batch_id | Entero | Micro-lote de Spark que procesó el evento. | 29 |

## Notas

- Este diccionario documenta la fuente cruda (RAW), tal como se captura en el
  punto de venta, antes de cualquier limpieza.
- Los valores válidos para los campos categóricos se detallan con sus reglas
  de cumplimiento en `contrato_datos.md`.
