import kagglehub
import pandas as pd
import os
from tqdm import tqdm
import time
import datetime
import nltk
from nltk.corpus import stopwords
import re

nltk.download('stopwords')
stop_words = set(stopwords.words('english'))

car_brands = [
    "Toyota", "Honda", "Ford", "Chevrolet", "Nissan", "Volkswagen", "BMW",
    "Mercedes-Benz", "Audi", "Hyundai", "Kia", "Subaru", "Mazda", "Tesla",
    "Renault", "Peugeot", "Fiat", "Volvo", "Jaguar", "Land Rover",
    "Mitsubishi", "Suzuki", "Chrysler", "Dodge", "Jeep", "Cadillac",
    "Lexus", "Acura", "Infiniti", "Skoda", "Seat", "Tata", "Mahindra", "Geely",
    "BYD", "Chery", "Great Wall", "SsangYong", "Proton", "Perodua", "Dacia",
    "Lada", "GAZ", "Ferrari", "Lamborghini", "Maserati", "Bugatti", "McLaren",
    "Aston Martin", "Pagani", "Koenigsegg", "Rolls-Royce", "Bentley", "Saab",
    "Hummer", "Pontiac", "Oldsmobile", "Mercury", "Rover", "Daewoo"
]

useful_columns = [
    "make_name", "model_name", "year", "description", "price", "daysonmarket",
    "city", "horsepower", "engine_displacement", "fuel_type", "transmission",
    "mileage", "body_type", "exterior_color", "interior_color", "engine_cylinders",
    "fuel_tank_volume", "wheelbase", "length", "width", "height",
    "maximum_seating", "owner_count", "salvage", "has_accidents",
    "frame_damaged", "is_cpo", "is_new", "is_oemcpo", "city_fuel_economy",
    "highway_fuel_economy"
]


def main():
    start_time = time.time()
    brand_map = {b.lower(): b for b in car_brands}
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_path = kagglehub.dataset_download("ananaymital/us-used-cars-dataset")
    csv_file = os.path.join(dataset_path, "used_cars_data.csv")
    print(f"Path to dataset file: {csv_file}")

    df = load_csv_with_progress(csv_file)
    df_clean = df[useful_columns]
    print("Pulizia del dataset in corso...")

    # Rimozione NaN da colonne critiche
    critical_columns = ["price", "make_name", "model_name", "year", "daysonmarket", "city", "description",
                        "engine_displacement", "horsepower"]
    df_clean = df_clean.dropna(subset=critical_columns)

    # Cast tipi numerici
    df_clean["price"] = pd.to_numeric(df_clean["price"], errors="coerce")
    df_clean["year"] = pd.to_numeric(df_clean["year"], errors="coerce")
    df_clean["engine_displacement"] = pd.to_numeric(df_clean["engine_displacement"], errors="coerce")
    df_clean["horsepower"] = pd.to_numeric(df_clean["horsepower"], errors="coerce")
    df_clean["daysonmarket"] = pd.to_numeric(df_clean["daysonmarket"], errors="coerce")

    df_clean = df_clean.dropna(subset=["price", "year", "engine_displacement", "horsepower", "daysonmarket"])
    df_clean = df_clean[df_clean["price"] >= 0]
    df_clean = df_clean[df_clean["engine_displacement"] >= 0]
    df_clean = df_clean[df_clean["horsepower"] >= 0]
    df_clean = df_clean[df_clean["daysonmarket"] >= 0]

    current_year = datetime.datetime.now().year
    df_clean = df_clean[(df_clean["year"] <= current_year + 1)]

    df_clean["make_name"] = (
        df_clean["make_name"]
        .str.strip()
        .str.lower()
        .map(brand_map)
    )
    df_clean = df_clean[df_clean["make_name"].notnull()]

    df_clean["model_name"] = df_clean["model_name"].astype(str)
    df_clean = df_clean[
        (df_clean["model_name"].str.len() < 50) &
        (~df_clean["model_name"].str.contains(r"[|:]", regex=True))
        ]

    df_clean["description"] = (
        df_clean["description"]
        .astype(str)
        .str.replace(r'\[\!@@.*?@@!\]', '', regex=True)
        .apply(remove_stopwords)
    )

    df_clean = df_clean.drop_duplicates()

    output_path = os.path.join(script_dir, "used_cars_data_clean.csv")
    df_clean.to_csv(output_path, index=False, encoding="utf-8")
    print(f"✅ Dataset pulito salvato in: {output_path}")
    print(f"⏱️ Tempo totale: {time.time() - start_time:.2f} secondi")

def remove_stopwords(text):
    words = re.findall(r'\b\w+\b', text.lower())  # tokenizza, rimuove punteggiatura
    filtered_words = [word for word in words if word not in stop_words]
    return ' '.join(filtered_words)

def load_csv_with_progress(file_path):
    chunks = []
    with open(file_path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f) - 1
    with tqdm(total=total_lines, desc="Lettura CSV") as pbar:
        for chunk in pd.read_csv(file_path, chunksize=10000, encoding='utf-8', low_memory=False):
            chunks.append(chunk)
            pbar.update(len(chunk))
    return pd.concat(chunks, ignore_index=True)


if __name__ == "__main__":
    main()
