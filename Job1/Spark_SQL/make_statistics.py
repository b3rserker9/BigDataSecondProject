from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg, count, collect_set

def main():
    spark = SparkSession.builder \
        .appName("MakeStatisticsJob") \
        .getOrCreate()

    # Legge il CSV pulito
    df = spark.read.option("header", True).option("inferSchema", True) \
        .csv("../../used_cars_data_clean.csv")

    # Statistiche per marca e modello
    stats_df = df.groupBy("make_name", "model_name").agg(
        count("*").alias("num_auto"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        avg("price").alias("avg_price"),
        collect_set("year").alias("years_present")
    )

    # Salvataggio in formato leggibile - CSV
    stats_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .option("delimiter", ";") \
        .csv("output/make_statistics_csv")

    # Mostra in console
    stats_df.show(truncate=False)

if __name__ == "__main__":
    main()
