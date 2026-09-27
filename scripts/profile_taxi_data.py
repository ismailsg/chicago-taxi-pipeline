from pyspark.sql import functions as F

from taxi_pipeline.resources.spark import SparkResource, BRONZE_PATH


def main():
    spark = SparkResource().get_spark()

    print("\n===== SCHEMA =====")
    df = spark.read.parquet(BRONZE_PATH)
    df.printSchema()

    print("\n===== BASIC STATISTICS =====")

    df.select(
        F.count("*").alias("rows"),
        F.countDistinct("trip_id").alias("distinct_trip_id"),
        F.sum(
            F.col("trip_id").isNull().cast("int")
        ).alias("null_trip_id"),
    ).show()

    print("\n===== TRIP DURATION =====")

    df.select(
        F.min("trip_seconds").alias("min_seconds"),
        F.expr(
            """
            percentile_approx(
                trip_seconds,
                array(0.50, 0.90, 0.95, 0.99, 0.995, 0.999),
                10000
            )
            """
        ).alias("percentiles_seconds"),
        F.max("trip_seconds").alias("max_seconds"),
    ).show(truncate=False)

    print("\n===== TRIP DISTANCE =====")

    df.select(
        F.min("trip_miles").alias("min_miles"),
        F.expr(
            """
            percentile_approx(
                trip_miles,
                array(0.50, 0.90, 0.95, 0.99, 0.995, 0.999),
                10000
            )
            """
        ).alias("percentiles_miles"),
        F.max("trip_miles").alias("max_miles"),
    ).show(truncate=False)

    df = df.withColumn(
        "trip_speed_mph",
        F.when(
            F.col("trip_seconds") > 0,
            F.col("trip_miles")
            / (F.col("trip_seconds") / 3600),
        ),
    )

    print("\n===== TRIP SPEED =====")

    df.select(
        F.min("trip_speed_mph").alias("min_speed"),
        F.expr(
            """
            percentile_approx(
                trip_speed_mph,
                array(0.50, 0.90, 0.95, 0.99, 0.995, 0.999),
                10000
            )
            """
        ).alias("percentiles_speed"),
        F.max("trip_speed_mph").alias("max_speed"),
    ).show(truncate=False)

    print("\n===== EXTREME DISTANCES =====")

    df.orderBy(
        F.col("trip_miles").desc()
    ).select(
        "trip_id",
        "trip_seconds",
        "trip_miles",
        "trip_speed_mph",
        "trip_total",
    ).show(20, truncate=False)

    print("\n===== EXTREME SPEEDS =====")

    df.orderBy(
        F.col("trip_speed_mph").desc()
    ).select(
        "trip_id",
        "trip_seconds",
        "trip_miles",
        "trip_speed_mph",
        "trip_total",
    ).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()