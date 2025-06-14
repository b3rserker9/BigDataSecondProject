#!/usr/bin/env python3
import sys
import csv
import re

def get_price_range(price):
    try:
        price = float(price)
        if price > 50000:
            return "alto"
        elif price >= 20000:
            return "medio"
        else:
            return "basso"
    except:
        return None

reader = csv.DictReader(sys.stdin)
count = 0
errors = 0

for row in reader:
    try:
        city = row.get("city", "").strip()
        year = row.get("year", "").strip()
        price = row.get("price", "").strip()
        days = row.get("daysonmarket", "").strip()
        desc = row.get("description", "").strip()

        price_range = get_price_range(price)
        if not price_range or not city or not year or not days:
            continue

        print(f"{city}\t{year}\t{price_range}\tSTAT\t1\t{days}")

        words = re.findall(r'\b\w+\b', desc.lower())
        for word in words:
            print(f"{city}\t{year}\t{price_range}\tWORD\t{word}\t1")

        count += 1
        if count % 1000 == 0:
            print(f"[LOG] Processed {count} records", file=sys.stderr)
    except Exception as e:
        errors += 1
        print(f"[ERROR] Exception processing line: {e}", file=sys.stderr)


print(f"[LOG] Finished processing. Total records: {count}, errors: {errors}", file=sys.stderr)
