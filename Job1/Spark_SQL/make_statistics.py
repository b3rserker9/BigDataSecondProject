from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import collect_list, struct, col, min, max, avg, count, collect_set, round
import os
import time

def process_file(spark, file_path, output_folder):
    start = time.time()
    file_name = os.path.splitext(os.path.basename(file_path))[0]

    print(f"\n📂 Elaborazione: {file_name}")
    df = spark.read.csv(
        file_path,
        header=True,
        inferSchema=True,
        multiLine=True,
        escape='"',
        quote='"'
    )

    df = df.withColumn("year", col("year").cast(IntegerType()))

    df_agg = df.groupBy("make_name", "model_name").agg(
        count("*").alias("num_cars"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        round(avg("price"), 2).alias("avg_price"),
        collect_set("year").alias("years_present")
    )

    df_final = df_agg.groupBy("make_name").agg(
        collect_list(
            struct(
                col("model_name"),
                col("num_cars"),
                col("min_price"),
                col("max_price"),
                col("avg_price"),
                col("years_present")
            )
        ).alias("models")
    )

    df_final.show(10, truncate=False)
    end = time.time()
    print(f"✅ Completato '{file_name}' in {end - start:.2f} secondi")
    print(f"✅ salvataggio del file...")
    df_final.write.mode("overwrite").json(f"{output_folder}/{file_name}_report.json")
    end = time.time()
    print(f"✅ salvataggio del file '{file_name}' in {end - start:.2f} secondi")



def main():
    total_start = time.time()
    input_folder = "../../"
    output_folder = "make_statistics_output/"
    os.makedirs(output_folder, exist_ok=True)

    spark = SparkSession.builder \
        .appName("MakeStatisticsJobBatch") \
        .getOrCreate()

    file_list = [f for f in os.listdir(input_folder) if f.endswith(".csv")]

    for file in file_list:
        process_file(spark, os.path.join(input_folder, file), output_folder)

    total_end = time.time()
    print(f"\n⏱️ Tempo totale di esecuzione batch: {total_end - total_start:.2f} secondi")


if __name__ == "__main__":
    main()
