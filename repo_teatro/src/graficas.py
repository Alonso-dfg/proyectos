"""
Genera las figuras del informe a partir de las salidas reales del proyecto
(capa GOLD batch, evidencias del streaming y comparación Pandas/PySpark).

Ejecución:  python3 src/graficas.py   ->  evidencia/figuras/*.png
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
import pandas as pd

from comun import GOLD, LOGS, RAIZ, UMBRAL_ALERTA_OCUPACION

FIG = RAIZ / "evidencia" / "figuras"
LS = RAIZ / "evidencia" / "logs_streaming"

# Paleta categórica validada (orden fijo) + tintas de texto
AZUL, NARANJA, AQUA, AMARILLO = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
GRIS, CRITICO = "#9a9890", "#d03b3b"
TXT, TXT2, REJILLA = "#0b0b0b", "#52514e", "#e6e5e0"
COLOR_CANAL = {"Taquilla": AZUL, "En linea": NARANJA, "Convenio": AQUA,
               "No especificado": GRIS}

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 10, "axes.edgecolor": REJILLA,
    "axes.labelcolor": TXT2, "xtick.color": TXT2, "ytick.color": TXT2,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": REJILLA, "grid.linewidth": 0.8,
    "axes.axisbelow": True, "figure.dpi": 200, "savefig.bbox": "tight",
    "axes.titleweight": "bold", "axes.titlesize": 12, "axes.titlecolor": TXT,
    "axes.titlelocation": "left", "legend.frameon": False,
})


def miles(x, _=None):
    return f"{x:,.0f}".replace(",", ".")


def guardar(fig, nombre):
    FIG.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG / nombre, facecolor="white")
    plt.close(fig)
    print("  ", nombre)


def arquitectura():
    fig, ax = plt.subplots(figsize=(11, 5.6))
    ax.set_xlim(-2, 110); ax.set_ylim(2, 57); ax.axis("off")

    def caja(x, y, w, h, titulo, sub, fondo, borde, color_t=TXT):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.5",
                                    fc=fondo, ec=borde, lw=1.4))
        ax.text(x + w / 2, y + h * 0.64, titulo, ha="center", va="center",
                fontsize=9.5, fontweight="bold", color=color_t)
        ax.text(x + w / 2, y + h * 0.3, sub, ha="center", va="center",
                fontsize=7.6, color=TXT2 if color_t == TXT else "#e8eefc")

    def flecha(x1, y1, x2, y2, txt=None):
        ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                    arrowprops=dict(arrowstyle="-|>", color=TXT2, lw=1.3))
        if txt:
            ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 1.4, txt, ha="center",
                    fontsize=7, color=TXT2)

    ax.text(20, 54, "BATCH (cierre diario, cifra oficial)", fontsize=9,
            fontweight="bold", color=AZUL)
    ax.text(20, 23, "STREAMING (operación en tiempo real)", fontsize=9,
            fontweight="bold", color=NARANJA)
    # Fuente común
    caja(0, 31, 15, 15, "FUENTE", "Taquilla POS\nWeb · Convenios", "#f3f2ee", GRIS)
    # Batch
    caja(20, 40, 15, 11, "RAW / BRONZE", "CSV diario\nsin modificar", "#eaf2fc", AZUL)
    caja(40, 40, 17, 11, "STAGING / SILVER", "Parquet por fecha\n+ cuarentena", "#eaf2fc", AZUL)
    caja(62, 40, 18, 11, "GOLD", "Estrella: hecho +\n4 dimensiones", "#eaf2fc", AZUL)
    caja(86, 40, 22, 11, "CONSUMO", "Dirección administrativa\nProgramación artística",
         "#14213d", "#14213d", "white")
    # Streaming
    caja(20, 8, 15, 12, "Kafka topic", "ventas_boletas\n3 particiones", "#fdeee7", NARANJA)
    caja(40, 8, 17, 12, "Spark Structured\nStreaming", "\n\ncontrato + watermark\n+ deduplicación",
         "#fdeee7", NARANJA)
    caja(62, 8, 18, 12, "SILVER / GOLD vivo", "ocupación · ventanas\nalertas ≥ 90 %", "#fdeee7", NARANJA)
    # Contrato compartido
    ax.add_patch(FancyBboxPatch((40, 27), 17, 7, boxstyle="round,pad=0.3,rounding_size=1.2",
                                fc="#fff6dd", ec=AMARILLO, lw=1.2, ls="--"))
    ax.text(48.5, 30.5, "src/contrato.py\nmismas reglas", ha="center", va="center",
            fontsize=7.6, color=TXT)
    flecha(15, 42, 20, 45.5)
    ax.text(4, 49, "export diario", fontsize=7, color=TXT2)
    flecha(15, 35, 20, 14)
    ax.text(4, 24, "eventos\nen línea", fontsize=7, color=TXT2)
    flecha(35, 45.5, 40, 45.5); flecha(57, 45.5, 62, 45.5); flecha(80, 45.5, 86, 45.5)
    flecha(35, 14, 40, 14); flecha(57, 14, 62, 14)
    flecha(80, 14, 97, 40)
    ax.text(91, 24, "alertas", fontsize=7, color=TXT2)
    flecha(48.5, 34, 48.5, 40); flecha(48.5, 27, 48.5, 20)
    flecha(71, 20, 71, 40, "conciliación")
    guardar(fig, "fig_arquitectura.png")


def ocupacion():
    d = pd.read_csv(GOLD / "csv" / "ind_ocupacion_funcion.csv")
    fig, ax = plt.subplots(figsize=(10, 4.4))
    colores = [CRITICO if v >= UMBRAL_ALERTA_OCUPACION * 100 else AZUL
               for v in d["ocupacion_pct"]]
    x = range(len(d))
    ax.bar(x, d["ocupacion_pct"], color=colores, width=0.62)
    ax.axhline(90, color=TXT2, lw=1, ls="--")
    ax.text(len(d) - 0.4, 91.5, "umbral de alerta 90 %", ha="right", fontsize=8, color=TXT2)
    for i, v in zip(x, d["ocupacion_pct"]):
        ax.text(i, v + 1.5, f"{v:.0f}", ha="center", fontsize=8, color=TXT)
    ax.set_xticks(list(x), [f"F{f}\n{o.split()[-1][:8]}" for f, o in
                            zip(d["sk_funcion"], d["obra"])], fontsize=7.5)
    ax.set_ylim(0, 105); ax.set_ylabel("Ocupación (% del aforo de 150)")
    ax.set_title("Ocupación por función (capa GOLD batch)")
    ax.grid(axis="x", visible=False)
    guardar(fig, "fig_ocupacion_funcion.png")


def obra_y_canal():
    obra = pd.read_csv(GOLD / "csv" / "ind_por_obra.csv").sort_values("ingreso_cop")
    canal = pd.read_csv(GOLD / "csv" / "ind_por_canal.csv")
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 3.9), gridspec_kw={"width_ratios": [1.25, 1]})
    a1.barh(obra["obra"], obra["ingreso_cop"] / 1e6, color=AZUL, height=0.6)
    for y, (v, p) in enumerate(zip(obra["ingreso_cop"] / 1e6, obra["participacion_ingreso_pct"])):
        a1.text(v + 0.1, y, f"${v:.2f} M ({p:.0f} %)", va="center", fontsize=8, color=TXT)
    a1.set_xlim(0, 10.5); a1.set_xlabel("Ingreso (millones COP)")
    a1.set_title("Ingreso por obra"); a1.grid(axis="y", visible=False)
    a2.bar(canal["canal_venta"], canal["boletas"],
           color=[COLOR_CANAL[c] for c in canal["canal_venta"]], width=0.6)
    for i, (b, p) in enumerate(zip(canal["boletas"], canal["pct_boletas"])):
        a2.text(i, b + 8, f"{b} ({p:.0f} %)", ha="center", fontsize=8, color=TXT)
    a2.set_ylabel("Boletas"); a2.set_title("Boletas por canal de venta")
    a2.tick_params(axis="x", labelsize=8); a2.grid(axis="x", visible=False)
    a2.set_ylim(0, 600)
    guardar(fig, "fig_obra_canal.png")


def latencia():
    est = pd.read_csv(LS / "metricas_microlotes.csv")
    sat = pd.read_csv(LS / "escenario_saturado" / "metricas_microlotes.csv")
    fig, ax = plt.subplots(figsize=(10, 3.9))
    ax.plot(range(len(sat)), sat["latencia_prom_s"], color=NARANJA, lw=2,
            label="Saturado: 1 lote cada 1,5 s")
    ax.plot(range(len(est)), est["latencia_prom_s"], color=AZUL, lw=2,
            label="Estable: 1 lote cada 3 s")
    ax.set_xlabel("Micro-lote con datos (n.º)"); ax.set_ylabel("Latencia promedio (s)")
    ax.set_title("Latencia evento → procesamiento por micro-lote")
    ax.legend(loc="upper left", fontsize=8)
    guardar(fig, "fig_latencia_streaming.png")


def evolucion_ocupacion():
    h = pd.read_csv(LS / "historial_ocupacion.csv")
    batches = sorted(h["batch_id"].unique())
    orden = {b: i for i, b in enumerate(batches)}
    h["paso"] = h["batch_id"].map(orden)
    fig, ax = plt.subplots(figsize=(10, 4))
    series = [(4, "F4 Bodas de Sangre (07-jul)", AZUL),
              (14, "F14 La Celestina (27-jul)", NARANJA),
              (12, "F12 Antígona (23-jul)", AQUA),
              (15, "F15 La Celestina (29-jul)", AMARILLO)]
    for f, nombre, c in series:
        s = h[h["id_funcion"] == f]
        ax.plot(s["paso"], s["ocupacion_pct"], color=c, lw=2, label=nombre)
    ax.axhline(90, color=CRITICO, lw=1, ls="--")
    ax.text(63, 86, "alerta 90 %", fontsize=8, color=TXT2, ha="right")
    ax.set_ylim(0, 105); ax.set_xlabel("Actualización del micro-lote (n.º)")
    ax.set_ylabel("Ocupación acumulada (%)")
    ax.set_title("Ocupación acumulada en tiempo real (funciones de mayor demanda)")
    ax.legend(loc="center left", fontsize=8)
    guardar(fig, "fig_ocupacion_tiempo_real.png")


def ventas_diarias():
    v = pd.read_csv(LS / "ventas_dia_canal.csv", parse_dates=["inicio_ventana"])
    p = (v.pivot_table(index="inicio_ventana", columns="canal_venta", values="boletas",
                       aggfunc="sum").fillna(0))
    p = p[[c for c in COLOR_CANAL if c in p.columns]]
    fig, ax = plt.subplots(figsize=(10, 3.9))
    abajo = None
    for c in p.columns:
        ax.bar(p.index, p[c], bottom=abajo, color=COLOR_CANAL[c], width=0.8,
               label=c, edgecolor="white", linewidth=0.6)
        abajo = p[c] if abajo is None else abajo + p[c]
    ax.set_ylabel("Boletas vendidas"); ax.set_title(
        "Ventas por día (ventana de 1 día, event time) y canal")
    ax.legend(ncol=4, fontsize=8, loc="upper left")
    ax.grid(axis="x", visible=False)
    fig.autofmt_xdate()
    guardar(fig, "fig_ventas_diarias_streaming.png")


def pandas_spark():
    c = pd.read_csv(LOGS / "comparacion_pandas_pyspark.csv")
    fig, ax = plt.subplots(figsize=(7.5, 3.6))
    x = range(len(c)); w = 0.36
    ax.bar([i - w / 2 - 0.01 for i in x], c["pandas_s"], w, color=AZUL, label="Pandas")
    ax.bar([i + w / 2 + 0.01 for i in x], c["pyspark_s"], w, color=NARANJA, label="PySpark (local[*])")
    for i, (a, b) in enumerate(zip(c["pandas_s"], c["pyspark_s"])):
        ax.text(i - w / 2, a + 0.01, f"{a:.2f}", ha="center", fontsize=8)
        ax.text(i + w / 2, b + 0.01, f"{b:.2f}", ha="center", fontsize=8)
    ax.set_xticks(list(x), [f"{f}\n{miles(r)} filas" for f, r in zip(c["factor"], c["registros"])])
    ax.set_ylabel("Tiempo (s)"); ax.set_title("Mismo indicador: Pandas vs. PySpark")
    ax.legend(fontsize=8); ax.grid(axis="x", visible=False)
    guardar(fig, "fig_pandas_vs_pyspark.png")


if __name__ == "__main__":
    print("Generando figuras en evidencia/figuras/")
    arquitectura(); ocupacion(); obra_y_canal(); pandas_spark()
    if (LS / "metricas_microlotes.csv").exists():
        evolucion_ocupacion(); ventas_diarias()
        if (LS / "escenario_saturado" / "metricas_microlotes.csv").exists():
            latencia()
