"""
Configuración común del proyecto (rutas, catálogos y sesión de Spark).

Se centraliza aquí para que el pipeline batch, la comparación Pandas/PySpark
y el consumidor de streaming usen exactamente las mismas reglas del contrato
de datos (docs/contrato_datos.md, v2).
"""
from pathlib import Path

# ---------------------------------------------------------------------------
# Rutas (relativas a la raíz del repositorio)
# ---------------------------------------------------------------------------
RAIZ = Path(__file__).resolve().parents[1]
DATA = RAIZ / "data"
RAW_CSV = DATA / "raw" / "ventas_boletas_raw.csv"
REFERENCIA = DATA / "reference"
CATALOGO_FUNCIONES = REFERENCIA / "catalogo_funciones.csv"
STAGING = DATA / "staging"
CUARENTENA = DATA / "quarantine"
GOLD = DATA / "gold"
STREAM = DATA / "stream"
LOGS = RAIZ / "evidencia" / "logs_calidad"

# ---------------------------------------------------------------------------
# Catálogos y reglas del contrato
# ---------------------------------------------------------------------------
# Catálogo oficial de obras: clave normalizada (minúsculas, sin espacios
# extra) -> nombre canónico.
CATALOGO_OBRAS = {
    "antigona": "Antigona",
    "bodas de sangre": "Bodas de Sangre",
    "el avaro": "El Avaro",
    "la casa de bernarda alba": "La Casa de Bernarda Alba",
    "la celestina": "La Celestina",
}
TIPOS_ENTRADA = ["General", "Preferencial", "Cortesia"]
CANALES = ["Taquilla", "En linea", "Convenio"]
CANAL_NO_ESPECIFICADO = "No especificado"
FECHA_MINIMA = "2024-01-01"

# Tarifa vigente por tipo de entrada (COP). Se usa solo como referencia de
# validación; el contrato NO permite imputar precios automáticamente.
TARIFAS = {"General": 25000, "Preferencial": 40000, "Cortesia": 0}

# Umbral para la alerta operativa de ocupación en streaming.
UMBRAL_ALERTA_OCUPACION = 0.90


def crear_spark(nombre_app: str, paquetes: str | None = None):
    """Crea (o reutiliza) una SparkSession en modo local.

    `local[*]` usa todos los núcleos del equipo: el mismo código correría en
    un clúster cambiando únicamente el `master` (YARN, Kubernetes, standalone).
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(nombre_app)
        .master("local[*]")
        # Pocos datos -> pocas particiones de shuffle (el valor por defecto,
        # 200, genera cientos de tareas vacías en un portátil).
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "America/Bogota")
        .config("spark.ui.showConsoleProgress", "false")
        # Parseo estricto de fechas: un texto que no cumple el patrón devuelve
        # null en vez de "adivinar" (evita confundir día y mes).
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )
    if paquetes:
        builder = builder.config("spark.jars.packages", paquetes)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
