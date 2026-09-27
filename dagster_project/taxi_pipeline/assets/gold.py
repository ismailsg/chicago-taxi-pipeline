from dagster import asset, AssetExecutionContext
from pyspark.sql import functions as F
from taxi_pipeline.resources import (
    SparkResource,
    SILVER_PATH,
    GOLD_REVENUE_BY_DAY,
    GOLD_AVG_TRIP_DURATION,
    GOLD_TOP_PICKUP_ZONES,
)


@asset(
    group_name="gold",
    deps=["silver_taxi_trips"],
)
def gold_revenue_by_day(
    context: AssetExecutionContext,
    spark: SparkResource,
):
    """Agrège les revenus et le nombre de trajets par jour."""

    s = spark.get_spark()

    df = s.read.parquet(SILVER_PATH)

    result = (
        df.groupBy("trip_date")
        .agg(
            F.count("*").alias("nb_trips"),
            F.sum("fare").alias("total_fare"),
            F.sum("tips").alias("total_tips"),
            F.sum("tolls").alias("total_tolls"),
            F.sum("extras").alias("total_extras"),
            F.sum("trip_total").alias("total_revenue"),
            F.avg("trip_total").alias("avg_trip_total"),
        )
        .orderBy("trip_date")
    )

    context.log.info(
        f"Nombre de jours dans Gold : {result.count()}"
    )

    result.write.mode("overwrite").parquet(
        GOLD_REVENUE_BY_DAY
    )

    context.log.info(
        f"Données écrites dans : {GOLD_REVENUE_BY_DAY}"
    )

    return None


@asset(
    group_name="gold",
    deps=["silver_taxi_trips"],
)
def gold_avg_trip_duration(
    context: AssetExecutionContext,
    spark: SparkResource,
):
    """Agrège les indicateurs de durée des trajets par jour."""

    s = spark.get_spark()

    df = s.read.parquet(SILVER_PATH)

    df = df.withColumn(
        "trip_minutes",
        F.col("trip_seconds") / 60.0,
    )

    result = (
        df.groupBy("trip_date")
        .agg(
            F.count("*").alias("nb_trips"),
            F.avg("trip_minutes").alias("avg_trip_minutes"),
            F.expr("percentile_approx(trip_minutes, 0.5)")
            .alias("median_trip_minutes"),
            F.expr("percentile_approx(trip_minutes, 0.9)")
            .alias("p90_trip_minutes"),
            F.avg("trip_miles").alias("avg_trip_miles"),
        )
        .orderBy("trip_date")
    )

    context.log.info(
        f"Nombre de jours dans Gold : {result.count()}"
    )

    result.write.mode("overwrite").parquet(
        GOLD_AVG_TRIP_DURATION
    )

    context.log.info(
        f"Données écrites dans : {GOLD_AVG_TRIP_DURATION}"
    )

    return None


@asset(
    group_name="gold",
    deps=["silver_taxi_trips"],
)
def gold_top_pickup_zones(
    context: AssetExecutionContext,
    spark: SparkResource,
):
    """Agrège les trajets par zone de prise en charge."""

    s = spark.get_spark()

    df = s.read.parquet(SILVER_PATH)

    result = (
        df.groupBy("pickup_community_area")
        .agg(
            F.count("*").alias("nb_trips"),
            F.sum("trip_total").alias("total_revenue"),
            F.avg("trip_total").alias("avg_trip_total"),
            F.avg("trip_seconds").alias("avg_trip_seconds"),
            F.avg("trip_miles").alias("avg_trip_miles"),
        )
        .orderBy(F.desc("nb_trips"))
    )

    context.log.info(
        f"Nombre de zones dans Gold : {result.count()}"
    )

    result.write.mode("overwrite").parquet(
        GOLD_TOP_PICKUP_ZONES
    )

    context.log.info(
        f"Données écrites dans : {GOLD_TOP_PICKUP_ZONES}"
    )

    return None
