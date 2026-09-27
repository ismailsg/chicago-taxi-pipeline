from dagster import asset, AssetExecutionContext
from taxi_pipeline.resources import SparkResource, BRONZE_PATH
from pyspark.sql import functions as F

# Le CSV brut est déposé par scripts/download_data.py dans ./data (monté en local_data)
RAW_CSV_PATH = "/opt/dagster/app/local_data/raw/taxi_trips_raw.csv"


@asset(group_name="bronze")
def bronze_taxi_trips(context: AssetExecutionContext, spark: SparkResource):
    """
    Ingestion brute : lit le CSV téléchargé via l'API SODA et l'écrit tel quel
    (schéma inféré, pas de nettoyage) en Parquet partitionné par date de trajet
    dans le bucket bronze.
    """
    s = spark.get_spark()

    df = s.read.option("header", True).option("inferSchema", True).csv(RAW_CSV_PATH)

    df = df.withColumn("trip_date", F.to_date("trip_start_timestamp"))

    context.log.info(f"Lignes ingérées en bronze : {df.count()}")

    (
        df.write.mode("overwrite")
        .partitionBy("trip_date")
        .parquet(BRONZE_PATH)
    )

    return None
