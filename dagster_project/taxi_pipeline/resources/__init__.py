import os
from dagster import ConfigurableResource
from pyspark.sql import SparkSession


class SparkResource(ConfigurableResource):
    """Fournit une SparkSession configurée pour lire/écrire sur un stockage S3."""

    app_name: str = "taxi-pipeline"

    def get_spark(self) -> SparkSession:
        s3_endpoint = os.environ.get("S3_ENDPOINT", "http://rustfs:9000")
        access_key = os.environ.get("S3_ACCESS_KEY", "rustfsadmin")
        secret_key = os.environ.get("S3_SECRET_KEY", "rustfsadmin")

        spark = (
            SparkSession.builder.appName(self.app_name)
            .master("local[*]")
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark


# Chemins des buckets, un seul endroit à modifier si besoin
BRONZE_PATH = "s3a://bronze/taxi_trips"
SILVER_PATH = "s3a://silver/taxi_trips_clean"
GOLD_REVENUE_BY_DAY = "s3a://gold/revenue_by_day"
GOLD_AVG_TRIP_DURATION = "s3a://gold/avg_trip_duration"
GOLD_TOP_PICKUP_ZONES = "s3a://gold/top_pickup_zones"
