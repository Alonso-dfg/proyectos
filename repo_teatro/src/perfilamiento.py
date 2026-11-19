import csv
from collections import Counter

path = "data/raw/ventas_boletas_raw.csv"

rows = list(csv.DictReader(open(path, encoding="utf-8")))
total = len(rows)
print(f"Total de registros: {total}")

fields = rows[0].keys()
print("\n-- Nulos / vacíos por campo --")
for f in fields:
    n_vacios = sum(1 for r in rows if r[f].strip() == "")
    pct = n_vacios / total * 100
    print(f"{f}: {n_vacios} ({pct:.1f}%)")

print("\n-- Duplicados por id_venta --")
ids = [r["id_venta"] for r in rows]
c = Counter(ids)
dups = {k: v for k, v in c.items() if v > 1}
print(f"id_venta duplicados: {len(dups)} valores, {sum(dups.values())} filas afectadas")

print("\n-- Formatos de fecha inconsistentes --")
malformed = sum(1 for r in rows if "/" in r["fecha_funcion"])
print(f"Fechas en formato DD/MM/YYYY (no estándar): {malformed}")

print("\n-- Inconsistencias en 'obra' (espacios/mayúsculas) --")
obras_raw = set(r["obra"] for r in rows)
print(f"Valores distintos literales de 'obra': {len(obras_raw)} (se esperaban 5 obras únicas)")

print("\n-- Reglas de negocio: Cortesia debe tener precio 0 --")
viol = sum(1 for r in rows if r["tipo_entrada"] == "Cortesia" and r["precio"] != "0")
viol2 = sum(1 for r in rows if r["tipo_entrada"] != "Cortesia" and r["precio"] == "0")
print(f"Cortesias con precio != 0: {viol}")
print(f"No-cortesias con precio == 0 (probable error): {viol2}")

print("\n-- Valores únicos por campo categórico --")
for f in ["tipo_entrada", "canal_venta"]:
    vals = Counter(r[f] for r in rows)
    print(f"{f}: {dict(vals)}")

print("\n-- Rango de precios --")
precios = [int(r["precio"]) for r in rows]
print(f"min={min(precios)} max={max(precios)}")
