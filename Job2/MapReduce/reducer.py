#!/usr/bin/env python3
import sys
from collections import Counter

current_key = None
total_cars = 0
total_days = 0
word_counter = Counter()

def emit_result(city, year, price_range, count, avg_days, top_words):
    top_words_str = ", ".join([w for w, _ in top_words])
    print(f"{city}\t{year}\t{price_range}\t{count}\t{avg_days:.2f}\t{top_words_str}")

for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) < 5:
        continue

    city = parts[0].strip()
    year = parts[1].strip()
    price_range = parts[2].strip()
    tag = parts[3]
    key = (city, year, price_range)

    if current_key != key:
        if current_key is not None:
            emit_result(
                current_key[0], current_key[1], current_key[2],
                total_cars,
                total_days / total_cars if total_cars > 0 else 0,
                word_counter.most_common(3)
            )
        current_key = key
        total_cars = 0
        total_days = 0
        word_counter = Counter()

    if tag == "STAT":
        try:
            total_cars += int(parts[4])
            total_days += float(parts[5])
        except:
            continue
    elif tag == "WORD":
        try:
            word = parts[4]
            count = int(parts[5])
            word_counter[word] += count
        except:
            continue

# Emissione finale dopo aver processato tutte le righe
if current_key is not None:
    emit_result(
        current_key[0], current_key[1], current_key[2],
        total_cars,
        total_days / total_cars if total_cars > 0 else 0,
        word_counter.most_common(3)
    )
