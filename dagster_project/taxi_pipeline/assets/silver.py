
from dagster import asset, AssetExecutionContext, Config, MetadataValue
from pyspark.sql import functions as F
from pyspark import StorageLevel
from pyspark.sql.window import Window

from taxi_pipeline.resources.spark import (
    SparkResource,
    BRONZE_PATH,
    SILVER_PATH,
    QUARANTINE_PATH,
)


# ---------------------------------------------------------------------------
# Règles métier définies à partir du profiling du dataset
# ---------------------------------------------------------------------------

class SilverConfig(Config):
    min_trip_seconds: int = 1
    max_trip_seconds: int = 6 * 3600
    min_trip_miles: float = 0
    max_trip_miles: float = 100
    max_speed_mph: float = 100


REQUIRED_BRONZE_COLUMNS = {
    "trip_id",
    "trip_start_timestamp",
    "trip_end_timestamp",
    "trip_seconds",
    "trip_miles",
    "fare",
    "tips",
    "tolls",
    "extras",
    "trip_total",
    "pickup_community_area",
    "dropoff_community_area",
    "trip_date",
}


@asset(
    group_name="silver",
    deps=["bronze_taxi_trips"],
)
def silver_taxi_trips(
    context: AssetExecutionContext,
    spark: SparkResource,
    config: SilverConfig,
):
    """
    Nettoyage et normalisation des trajets taxi.

    Grain :
        1 ligne = 1 trajet

    Transformations :
        - typage explicite des colonnes
        - déduplication déterministe sur trip_id
        - étiquetage en une passe des motifs de rejet
        - calcul de la vitesse moyenne
        - écriture Silver partitionnée
        - écriture de la quarantaine partitionnée
        - vérification du volume écrit
    """

    s = spark.get_spark()

    # -----------------------------------------------------------------------
    # 1. Lecture Bronze
    # -----------------------------------------------------------------------

    df = s.read.parquet(BRONZE_PATH)

    total_bronze = df.count()

    missing_columns = sorted(REQUIRED_BRONZE_COLUMNS - set(df.columns))
    if missing_columns:
        raise ValueError(
            "Le schéma Bronze est incomplet. Colonnes manquantes : "
            + ", ".join(missing_columns)
        )

    if total_bronze == 0:
        raise ValueError("Le jeu de données Bronze est vide")

    context.log.info(
        f"Nombre de lignes en Bronze : {total_bronze:,}"
    )

    # -----------------------------------------------------------------------
    # 2. Typage explicite
    # -----------------------------------------------------------------------

    df = (
        df
        .withColumn(
            "trip_start_timestamp",
            F.to_timestamp("trip_start_timestamp"),
        )
        .withColumn(
            "trip_end_timestamp",
            F.to_timestamp("trip_end_timestamp"),
        )
        .withColumn(
            "trip_seconds",
            F.col("trip_seconds").cast("long"),
        )
        .withColumn(
            "trip_miles",
            F.col("trip_miles").cast("double"),
        )
        .withColumn(
            "fare",
            F.col("fare").cast("double"),
        )
        .withColumn(
            "tips",
            F.col("tips").cast("double"),
        )
        .withColumn(
            "tolls",
            F.col("tolls").cast("double"),
        )
        .withColumn(
            "extras",
            F.col("extras").cast("double"),
        )
        .withColumn(
            "trip_total",
            F.col("trip_total").cast("double"),
        )
    )

    # -----------------------------------------------------------------------
    # 3. Déduplication déterministe
    # -----------------------------------------------------------------------

    duplicate_window = (
        Window
        .partitionBy("trip_id")
        .orderBy(
            F.col("trip_end_timestamp").desc_nulls_last(),
            F.col("trip_start_timestamp").desc_nulls_last(),
            F.col("trip_total").desc_nulls_last(),
        )
    )

    df = df.withColumn(
        "_duplicate_rank",
        F.row_number().over(duplicate_window),
    )

    # -----------------------------------------------------------------------
    # 4-9. Calcul de la vitesse et étiquetage d'une seule raison de rejet
    # -----------------------------------------------------------------------

    df = df.withColumn(
        "trip_speed_mph",
        F.col("trip_miles")
        / (F.col("trip_seconds") / F.lit(3600)),
    )

    df = df.withColumn(
        "reject_reason",
        F.when(F.col("trip_id").isNull(), "null_trip_id")
        .when(F.col("_duplicate_rank") > 1, "duplicate_trip_id")
        .when(
            F.col("trip_start_timestamp").isNull()
            | F.col("trip_end_timestamp").isNull()
            | (F.col("trip_end_timestamp") < F.col("trip_start_timestamp")),
            "invalid_timestamp",
        )
        .when(
            F.col("trip_seconds").isNull()
            | (F.col("trip_seconds") < config.min_trip_seconds)
            | (F.col("trip_seconds") > config.max_trip_seconds),
            "invalid_duration",
        )
        .when(
            F.col("trip_miles").isNull()
            | (F.col("trip_miles") < config.min_trip_miles)
            | (F.col("trip_miles") > config.max_trip_miles),
            "invalid_distance",
        )
        .when(
            F.col("trip_speed_mph").isNull()
            | (F.col("trip_speed_mph") > config.max_speed_mph),
            "invalid_speed",
        )
        .when(
            ((F.col("fare").isNotNull()) & (F.col("fare") < 0))
            | ((F.col("tips").isNotNull()) & (F.col("tips") < 0))
            | ((F.col("tolls").isNotNull()) & (F.col("tolls") < 0))
            | ((F.col("extras").isNotNull()) & (F.col("extras") < 0))
            | ((F.col("trip_total").isNotNull()) & (F.col("trip_total") < 0)),
            "negative_amount",
        )
        .when(
            F.col("pickup_community_area").isNull()
            | F.col("dropoff_community_area").isNull(),
            "missing_geography",
        )
        .otherwise("valid"),
    )

    df.persist(StorageLevel.DISK_ONLY)

    reject_counts = (
        df.groupBy("reject_reason")
        .count()
        .collect()
    )
    rejection_counts = {
        row["reject_reason"]: row["count"]
        for row in reject_counts
    }

    total_silver = rejection_counts.get("valid", 0)
    total_rejected = total_bronze - total_silver
    retention_rate = (total_silver / total_bronze) * 100

    for reason, count in sorted(rejection_counts.items()):
        if reason != "valid":
            context.log.info(
                f"Rejets {reason} : {count:,} lignes"
            )

    context.log.info(f"Nombre de lignes en Silver : {total_silver:,}")
    context.log.info(f"Nombre total rejeté : {total_rejected:,}")
    context.log.info(f"Taux de conservation : {retention_rate:.2f}%")

    quarantine = df.filter(F.col("reject_reason") != "valid")
    silver = (
        df.filter(F.col("reject_reason") == "valid")
        .drop("reject_reason", "_duplicate_rank")
    )

    if total_silver == 0:
        raise ValueError("Le traitement n'a produit aucune ligne Silver")

    try:
        s.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

        (
            quarantine.write
            .mode("overwrite")
            .partitionBy("reject_reason")
            .parquet(QUARANTINE_PATH)
        )

        s.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        (
            silver.write
            .mode("overwrite")
            .partitionBy("trip_date")
            .parquet(SILVER_PATH)
        )

        written_silver = s.read.parquet(SILVER_PATH).count()
        if written_silver != total_silver:
            raise ValueError(
                "Vérification d'écriture Silver échouée : "
                f"{written_silver:,} lignes relues pour "
                f"{total_silver:,} attendues"
            )
    finally:
        df.unpersist()

    context.log.info(
        f"Données Silver écrites dans : {SILVER_PATH}"
    )
    context.log.info(
        f"Données quarantaine écrites dans : {QUARANTINE_PATH}"
    )

    context.add_output_metadata(
        {
            "bronze_rows": MetadataValue.int(total_bronze),
            "silver_rows": MetadataValue.int(total_silver),
            "rejected_rows": MetadataValue.int(total_rejected),
            "retention_rate_pct": MetadataValue.float(retention_rate),
            "rejection_counts": MetadataValue.json(rejection_counts),
            "silver_path": MetadataValue.text(SILVER_PATH),
            "quarantine_path": MetadataValue.text(QUARANTINE_PATH),
        }
    )

    return None


