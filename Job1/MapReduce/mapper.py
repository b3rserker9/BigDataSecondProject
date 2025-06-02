#!/usr/bin/env python3
import sys
import csv
import json

# csv.DictReader legge da sys.stdin e interpreta la prima riga come header
reader = csv.DictReader(sys.stdin)

for row in reader:
    make = row.get("make_name", "").strip()
    model = row.get("model_name", "").strip()
    price_str = row.get("price", "").strip()
    year_str = row.get("year", "").strip()

    try:
        price = float(price_str)
        year = int(year_str)
    except ValueError:
        # Se il prezzo o anno non sono validi, skippo questa riga
        continue

    if make and model:
        key = f"{make}|{model}"
        value = json.dumps({"price": price, "year": year, "count": 1})
        print(f"{key}\t{value}")
