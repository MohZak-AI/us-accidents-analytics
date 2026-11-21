import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour, dayofweek, month, year, to_timestamp

def preprocess_accidents(df):
    """
    Reference ETL logic aligned with the final ADF pipeline.
    This script is kept for local validation and reproducibility support.
    The main ETL pipeline was implemented in Azure Data Factory.
    """

    # Convert Start_Time to timestamp
    df = df.withColumn(
        "Start_Time",
        to_timestamp(col("Start_Time"), "yyyy-MM-dd HH:mm:ss")
    )

    # Derived features aligned with ADF pipeline
    df = df.withColumn("accidentYear", year(col("Start_Time")))
    df = df.withColumn("hourOfDay", hour(col("Start_Time")))
    df = df.withColumn("month", month(col("Start_Time")))

    # Rush hour: weekdays, 7-9 AM and 4-6 PM
    df = df.withColumn(
        "isRushHour",
        when(
            (dayofweek(col("Start_Time")).between(2, 6)) &
            (
                (col("hourOfDay").between(7, 9)) |
                (col("hourOfDay").between(16, 18))
            ),
            1
        ).otherwise(0)
    )

    # Weather severity mapping aligned with ADF logic
    df = df.withColumn(
        "weatherSeverityMapped",
        when(col("Weather_Condition").isin("Fair", "Clear"), 1)
        .when(col("Weather_Condition").isin("Cloudy", "Fog"), 2)
        .when(col("Weather_Condition").isin("Light Rain", "Rain"), 3)
        .otherwise(4)
    )

    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare partitioned US Accidents Parquet features.")
    parser.add_argument("--input", required=True, help="Source CSV path or configured cloud URI")
    parser.add_argument("--output", required=True, help="Destination directory (overwritten)")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("US_Accidents_ETL_Reference")
             .config("spark.sql.parquet.compression.codec", "snappy").getOrCreate())
    try:
        raw = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input)
        processed = preprocess_accidents(raw)
        processed.write.mode("overwrite").partitionBy("accidentYear").parquet(args.output)
        print(f"Wrote partitioned Parquet to {args.output}")
    finally:
        spark.stop()
