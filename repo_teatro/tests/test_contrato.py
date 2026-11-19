"""
Pruebas automáticas del contrato de datos y de las salidas del pipeline.

Ejecución (desde la raíz del repositorio):
    python3 -m pytest tests/ -v

Las pruebas unitarias construyen registros "sucios" a mano y verifican que
src/contrato.py los normalice o los envíe a cuarentena con el motivo
correcto. Las pruebas de integración leen las salidas reales del batch y del
streaming (requieren haber ejecutado antes pipeline_batch_spark.py y
demo_streaming.sh).
"""
import json
import sys
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))

from comun import CANAL_NO_ESPECIFICADO, crear_spark  # noqa: E402
from contrato import aplicar_contrato  # noqa: E402

COLUMNAS = ["id_venta", "id_funcion", "obra", "fecha_funcion", "hora_funcion",
            "tipo_entrada", "canal_venta", "precio"]


@pytest.fixture(scope="session")
def spark():
    s = crear_spark("TeatroPruebas")
    yield s
    s.stop()


def evaluar(spark, **cambios):
    """Aplica el contrato a UN registro válido modificado con `cambios`."""
    base = {"id_venta": "9001", "id_funcion": "12", "obra": "Antigona",
            "fecha_funcion": "2026-07-23", "hora_funcion": "20:00",
            "tipo_entrada": "General", "canal_venta": "Taquilla",
            "precio": "25000"}
    base.update(cambios)
    df = spark.createDataFrame([tuple(base[c] for c in COLUMNAS)],
                               "id_venta string, id_funcion string, obra string, "
                               "fecha_funcion string, hora_funcion string, "
                               "tipo_entrada string, canal_venta string, "
                               "precio string")
    return aplicar_contrato(df).first()


# --------------------------- Pruebas unitarias ------------------------------
def test_registro_correcto_es_valido(spark):
    assert evaluar(spark)["motivos_cuarentena"] == []


def test_obra_con_espacios_y_mayusculas_se_normaliza(spark):
    r = evaluar(spark, obra="  ANTIGONA  ")
    assert r["obra"] == "Antigona" and r["motivos_cuarentena"] == []


def test_obra_fuera_de_catalogo_va_a_cuarentena(spark):
    r = evaluar(spark, obra="Hamlet")
    assert r["obra"] is None and "obra_fuera_catalogo" in r["motivos_cuarentena"]


def test_fecha_dd_mm_aaaa_se_corrige(spark):
    r = evaluar(spark, fecha_funcion="23/07/2026")
    assert str(r["fecha_funcion"]) == "2026-07-23" and r["fecha_corregida"]


def test_fecha_invalida_y_anterior_al_minimo(spark):
    assert "fecha_invalida" in evaluar(spark, fecha_funcion="2026-13-45")["motivos_cuarentena"]
    assert "fecha_invalida" in evaluar(spark, fecha_funcion="2020-01-01")["motivos_cuarentena"]


def test_canal_vacio_se_imputa_sin_perder_la_venta(spark):
    r = evaluar(spark, canal_venta="")
    assert r["canal_venta"] == CANAL_NO_ESPECIFICADO and r["motivos_cuarentena"] == []


def test_precio_cero_sin_cortesia_va_a_cuarentena(spark):
    assert evaluar(spark, precio="0")["motivos_cuarentena"] == ["precio_cero_no_cortesia"]


def test_cortesia_con_precio_va_a_cuarentena(spark):
    assert evaluar(spark, tipo_entrada="Cortesia")["motivos_cuarentena"] == ["cortesia_con_precio"]


def test_cortesia_en_cero_es_valida(spark):
    assert evaluar(spark, tipo_entrada="Cortesia", precio="0")["motivos_cuarentena"] == []


def test_tipo_desconocido_y_precio_no_numerico(spark):
    assert "tipo_sin_clasificar" in evaluar(spark, tipo_entrada="VIP")["motivos_cuarentena"]
    assert "precio_nulo_o_negativo" in evaluar(spark, precio="abc")["motivos_cuarentena"]


def test_id_nulo_va_a_cuarentena(spark):
    assert "id_nulo" in evaluar(spark, id_venta=None)["motivos_cuarentena"]


def test_varias_reglas_se_acumulan(spark):
    r = evaluar(spark, obra="Hamlet", fecha_funcion="xx", precio="0")
    assert {"obra_fuera_catalogo", "fecha_invalida",
            "precio_cero_no_cortesia"} <= set(r["motivos_cuarentena"])


def test_entrada_vacia_no_falla(spark):
    vacio = spark.createDataFrame([], ", ".join(f"{c} string" for c in COLUMNAS))
    assert aplicar_contrato(vacio).count() == 0


# ------------------------- Pruebas de integración ---------------------------
def test_cuadre_del_batch():
    log = json.loads((RAIZ / "evidencia/logs_calidad/log_calidad_batch.json")
                     .read_text(encoding="utf-8"))
    assert log["cuadre"] and log["registros_raw"] == (
        log["registros_staging"] + log["registros_cuarentena"]
        + log["filas_duplicadas_descartadas"])


def test_staging_sin_duplicados_y_gold_integro(spark):
    staging = spark.read.parquet(str(RAIZ / "data/staging/ventas_boletas"))
    assert staging.count() == staging.select("id_venta").distinct().count()
    hecho = spark.read.parquet(str(RAIZ / "data/gold/hecho_venta_boleta"))
    dim = spark.read.parquet(str(RAIZ / "data/gold/dim_funcion"))
    assert hecho.join(dim, "sk_funcion", "left_anti").count() == 0


def test_streaming_sin_duplicados_y_conciliado():
    c = json.loads((RAIZ / "evidencia/logs_streaming/resumen_streaming.json")
                   .read_text(encoding="utf-8"))["conciliacion_con_batch"]
    assert c["duplicados_en_streaming"] == 0
    assert c["ids_solo_en_streaming"] == []
    assert c["faltantes_explicados_por_eventos_muy_tardios"]
