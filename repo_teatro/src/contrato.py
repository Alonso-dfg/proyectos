"""
Reglas del contrato de datos (docs/contrato_datos.md) como transformaciones
de Spark reutilizables.

El MISMO código se aplica en el pipeline batch (pipeline_batch_spark.py) y en
el consumidor de streaming (streaming_ventas.py). Así una boleta se limpia y
valida igual sin importar si llega en el archivo diario o como evento en
tiempo real: el contrato es uno solo.

Todas las funciones son TRANSFORMACIONES (lazy): devuelven un DataFrame nuevo
y no ejecutan nada hasta que una acción o una consulta de streaming lo pide.
Ninguna usa operaciones que el streaming no soporte (por ejemplo, ventanas
row_number), por eso la deduplicación se resuelve aparte en cada modo.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from comun import (CANAL_NO_ESPECIFICADO, CATALOGO_OBRAS, FECHA_MINIMA,
                   TIPOS_ENTRADA)

VERSION_CONTRATO = "v3"


def _obra_canonica(clave):
    """Clave normalizada -> nombre canónico del catálogo (o null si no existe).

    Se construye un CASE WHEN con el catálogo en lugar de un join: el
    resultado equivale a un broadcast join, funciona idéntico en batch y en
    streaming y no requiere un DataFrame adicional. (Un `create_map` literal
    falló de forma intermitente con varias consultas de streaming en
    paralelo, por eso se prefirió esta forma.)
    """
    expr = None
    for k, canonica in CATALOGO_OBRAS.items():
        expr = (F.when(clave == k, F.lit(canonica)) if expr is None
                else expr.when(clave == k, F.lit(canonica)))
    return expr


def normalizar(df: DataFrame) -> DataFrame:
    """Tipado y normalización de campos (sin descartar ningún registro)."""
    return (df
            # Tipos
            .withColumn("id_venta", F.col("id_venta").cast("int"))              
            .withColumn("id_funcion", F.col("id_funcion").cast("int"))
            .withColumn("precio", F.col("precio").cast("long"))  # COP enteros
            # obra: quitar espacios extra y mayúsculas para cruzar con catálogo
            .withColumn("obra_original", F.col("obra"))
            .withColumn("obra", _obra_canonica(
                F.lower(F.trim(F.regexp_replace("obra", r"\s+", " ")))))
            # fecha: dos formatos conocidos, parseo estricto de cada uno
            .withColumn("fecha_original", F.col("fecha_funcion"))
            .withColumn("fecha_funcion", F.coalesce(
                F.to_date("fecha_funcion", "yyyy-MM-dd"),
                F.to_date("fecha_funcion", "d/M/yyyy")))
            .withColumn("fecha_corregida",
                        F.col("fecha_original").contains("/"))
            # canal vacío -> "No especificado" (acción del contrato)
            .withColumn("canal_imputado",
                        F.col("canal_venta").isNull()
                        | (F.trim("canal_venta") == ""))
            .withColumn("canal_venta",
                        F.when(F.col("canal_imputado"),
                               F.lit(CANAL_NO_ESPECIFICADO))
                         .otherwise(F.trim("canal_venta")))
            # tipo de entrada fuera del dominio -> "Sin clasificar"
            .withColumn("tipo_entrada",
                        F.when(F.trim("tipo_entrada").isin(TIPOS_ENTRADA),
                               F.trim("tipo_entrada"))
                         .otherwise(F.lit("Sin clasificar"))))


# Reglas que envían un registro a CUARENTENA (revisión manual).
def _reglas():
    return {
        "id_nulo": F.col("id_venta").isNull(),
        "obra_fuera_catalogo": F.col("obra").isNull(),
        "fecha_invalida": F.col("fecha_funcion").isNull()
                          | (F.col("fecha_funcion") < F.lit(FECHA_MINIMA)),
        "tipo_sin_clasificar": F.col("tipo_entrada") == "Sin clasificar",
        "precio_nulo_o_negativo": F.col("precio").isNull()
                                  | (F.col("precio") < 0),
        "cortesia_con_precio": (F.col("tipo_entrada") == "Cortesia")
                               & (F.col("precio") != 0),
        "precio_cero_no_cortesia": (F.col("tipo_entrada") != "Cortesia")
                                   & (F.col("precio") == 0),
    }


NOMBRES_REGLAS = ["id_nulo", "obra_fuera_catalogo", "fecha_invalida",
                  "tipo_sin_clasificar", "precio_nulo_o_negativo",
                  "cortesia_con_precio", "precio_cero_no_cortesia"]


def marcar_reglas(df: DataFrame) -> DataFrame:
    """Agrega `motivos_cuarentena` (array con las reglas incumplidas)."""
    motivos = [F.when(cond, F.lit(nombre)) for nombre, cond in _reglas().items()]
    return df.withColumn("motivos_cuarentena",
                         F.array_compact(F.array(*motivos)))


def aplicar_contrato(df: DataFrame) -> DataFrame:
    """normalizar + marcar_reglas. Devuelve todos los registros marcados."""
    return marcar_reglas(normalizar(df))


def es_valido():
    return F.size("motivos_cuarentena") == 0
