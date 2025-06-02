#!/usr/bin/env python3
import sys
import json

current_key = None

count = 0
sum_price = 0.0
min_price = None
max_price = None
years_set = set()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, value_json = line.split('\t', 1)
    try:
        value = json.loads(value_json)
    except json.JSONDecodeError:
        continue

    if current_key != key:
        # Se cambio chiave, output il risultato precedente
        if current_key is not None:
            avg_price = sum_price / count if count > 0 else 0
            years_list = sorted(years_set)
            print(f"{current_key}\t{count}\t{min_price}\t{max_price}\t{avg_price:.2f}\t{','.join(map(str, years_list))}")

        # Reset variabili per nuova chiave
        current_key = key
        count = 0
        sum_price = 0.0
        min_price = None
        max_price = None
        years_set = set()

    # Aggiorno aggregazioni
    count += value.get("count", 1)
    price = value.get("price", 0)
    year = value.get("year", None)

    sum_price += price

    if min_price is None or price < min_price:
        min_price = price
    if max_price is None or price > max_price:
        max_price = price
    if year is not None:
        years_set.add(year)

# Output ultima chiave
if current_key is not None:
    avg_price = sum_price / count if count > 0 else 0
    years_list = sorted(years_set)
    print(f"{current_key}\t{count}\t{min_price}\t{max_price}\t{avg_price:.2f}\t{','.join(map(str, years_list))}")
