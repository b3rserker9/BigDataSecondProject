from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct, col, min, max, avg, count, collect_set, round
import time


def main():
    start_time = time.time()
    spark = SparkSession.builder \
        .appName("MakeStatisticsJob") \
        .getOrCreate()

    # Legge il CSV pulito
    df = spark.read.csv("../../used_cars_data_clean.csv", header=True,   inferSchema=True,
    multiLine=True,
    escape='"',
    quote='"'
)

    # OPZIONE 1: Usa solo SQL (raccomandato)
    df_agg = df.groupBy("make_name", "model_name").agg(
        count("*").alias("num_cars"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        round(avg("price"), 2).alias("avg_price"),
        collect_set("year").alias("years_present")
    )

    # Raggruppa per marca e crea lista modelli
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

    # Salva in JSON
    df_final.write.mode("overwrite").json("output.json")
    df_final.write.mode("overwrite").parquet("car_staticsJob1.parquet")
    print(f"⏱️ Tempo totale: {time.time() - start_time:.2f} secondi")

if __name__ == "__main__":
    main()
