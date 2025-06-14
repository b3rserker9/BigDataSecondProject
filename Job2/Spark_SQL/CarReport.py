from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, avg, count, explode, split, lower, regexp_replace, row_number, \
    collect_list, concat_ws
from pyspark.sql.window import Window
import os
import time

# Inizio esecuzione globale
global_start = time.time()

spark = SparkSession.builder \
    .appName("CarReportBatch") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

input_folder = "../../"
output_folder = "output_reports/"
os.makedirs(output_folder, exist_ok=True)

file_list = [f for f in os.listdir(input_folder) if f.endswith(".csv")]

for file in file_list:
    file_start = time.time()
    file_path = os.path.join(input_folder, file)
    file_name = os.path.splitext(file)[0]

    print(f"\n🚗 Elaborazione: {file}")

    cars = spark.read.csv(
        file_path, header=True, inferSchema=True,
        multiLine=True, escape='"', quote='"'
    )

    cars = cars.withColumn(
        "price_range",
        when(col("price") > 50000, "alto")
        .when((col("price") >= 20000) & (col("price") <= 50000), "medio")
        .otherwise("basso")
    )

    agg_stats = cars.groupBy("city", "year", "price_range").agg(
        count("*").alias("num_cars"),
        avg("daysonmarket").alias("avg_daysonmarket")
    )

    words_df = cars.select(
        "city", "year", "price_range",
        explode(
            split(
                regexp_replace(lower(col("description")), "[^a-z0-9\\s]", ""), " "
            )
        ).alias("word")
    ).filter(col("word") != "")

    word_counts = words_df.groupBy("city", "year", "price_range", "word").count()
    window_spec = Window.partitionBy("city", "year", "price_range").orderBy(col("count").desc())
    word_ranked = word_counts.withColumn("rank", row_number().over(window_spec))
    top_words = word_ranked.filter(col("rank") <= 3)

    top_words_agg = top_words.groupBy("city", "year", "price_range").agg(
        concat_ws(", ", collect_list("word")).alias("top_3_words")
    )

    report = agg_stats.join(top_words_agg, ["city", "year", "price_range"], "left")

    report.show(10, truncate=False)
    end = time.time()
    print(f"✅ Completato '{file_name}' in {end - file_start:.2f} secondi")
    print(f"✅ salvataggio del file...")
    report.write.mode("overwrite").json(f"{output_folder}{file_name}_report.json")
    end = time.time()
    print(f"✅ salvataggio del file '{file_name}' in {end - file_start:.2f} secondi")

total_end = time.time()
print(f"\n⏱️ Tempo totale batch: {total_end - global_start:.2f} secondi")
