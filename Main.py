import kagglehub
import pandas as pd
import os
from tqdm import tqdm
import time
import datetime

useful_columns = [
    "make_name",
    "model_name",
    "year",
    "description",
    "price",
    "daysonmarket",
    "city",
    "horsepower",
    "engine_displacement",
    "fuel_type",
    "transmission",
    "mileage",
    "body_type",
    "exterior_color",
    "interior_color",
    "engine_cylinders",
    "fuel_tank_volume",
    "wheelbase",
    "length",
    "width",
    "height",
    "maximum_seating",
    "owner_count",
    "salvage",
    "has_accidents",
    "frame_damaged",
    "is_cpo",
    "is_new",
    "is_oemcpo",
    "city_fuel_economy",
    "highway_fuel_economy",
]


def main():
    start_time = time.time()
    # Directory script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Scarica dataset (ottieni path directory)
    dataset_path = kagglehub.dataset_download("ananaymital/us-used-cars-dataset")
    csv_file = os.path.join(dataset_path, "used_cars_data.csv")
    print(f"Path to dataset file: {csv_file}")

    # Carica CSV con low_memory=False e encoding
    df = load_csv_with_progress(csv_file)
    df_clean = df[useful_columns]
    print("il dataset sta venendo preparato(per esempio eliminando dati errati o non significativi)")
    columns_to_check = ["price", "make_name", "model_name", "year", "daysonmarket", "city", "description",
                        "engine_displacement", "horsepower"]
    print("Eliminazione righe con NaN in colonne importanti... ", columns_to_check)
    df_clean = df_clean.dropna(subset=columns_to_check)
    print("Eliminazione righe con interi maggiorni di 0... ")
    df_clean = df_clean[df_clean['price'] >= 0]
    current_year = datetime.datetime.now().year
    df_clean = df_clean[(df_clean['year'] <= current_year + 1)]
    df_clean = df_clean[df_clean['engine_displacement'] >= 0]
    df_clean = df_clean[df_clean['horsepower'] >= 0]
    df_clean = df_clean[df_clean['daysonmarket'] >= 0]
    print("Eliminazione tutto ciò che ha la forma [!@@...@@!] nella colonna description... ")
    df_clean["description"] = df_clean["description"].str.replace(r'\[\!@@.*?@@!\]', '', regex=True)
    # Rimuove duplicati
    print("Rimozione righe duplicate...")
    df_clean = df_clean.drop_duplicates()

    # Salva il dataset pulito nella stessa cartella dello script
    output_path = os.path.join(script_dir, "used_cars_data_clean.csv")
    df_clean.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Dataset pulito salvato in: {output_path}")
    elapsed = time.time() - start_time
    print(f"Tempo totale impiegato: {elapsed:.2f} secondi")


def load_csv_with_progress(file_path):
    chunksize = 10000  # numero di righe per ogni chunk
    chunks = []
    total_rows = 0

    # prima leggiamo il numero totale di righe per configurare la barra
    with open(file_path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f) - 1  # escludi header
    with tqdm(total=total_lines, desc="Lettura CSV") as pbar:
        for chunk in pd.read_csv(file_path, chunksize=chunksize, encoding='utf-8', low_memory=False):
            chunks.append(chunk)
            pbar.update(len(chunk))
    df = pd.concat(chunks, ignore_index=True)
    return df


if __name__ == "__main__":
    main()
