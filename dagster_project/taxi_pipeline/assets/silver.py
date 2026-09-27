
from dagster import asset, AssetExecutionContext
from pyspark.sql import functions as F

from taxi_pipeline.resources.spark import (
    SparkResource,
    BRONZE_PATH,
    SILVER_PATH,
)


# ---------------------------------------------------------------------------
# Règles métier définies à partir du profiling du dataset
# ---------------------------------------------------------------------------

MIN_TRIP_SECONDS = 1
MAX_TRIP_SECONDS = 6 * 3600       # 6 heures

MIN_TRIP_MILES = 0
MAX_TRIP_MILES = 100              # 100 miles

MAX_SPEED_MPH = 100


@asset(
    group_name="silver",
    deps=["bronze_taxi_trips"],
)
def silver_taxi_trips(
    context: AssetExecutionContext,
    spark: SparkResource,
):
    """
    Nettoyage et normalisation des trajets taxi.

    Grain :
        1 ligne = 1 trajet

    Transformations :
        - typage explicite des colonnes
        - suppression des doublons sur trip_id
        - validation de la durée
        - validation de la distance
        - calcul de la vitesse moyenne
        - suppression des vitesses incohérentes
        - validation des montants
        - validation des zones géographiques
    """

    s = spark.get_spark()

    # -----------------------------------------------------------------------
    # 1. Lecture Bronze
    # -----------------------------------------------------------------------

    df = s.read.parquet(BRONZE_PATH)

    total_bronze = df.count()

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
    # 3. Identifiant du trajet
    # -----------------------------------------------------------------------

    before = df.count()

    df = (
        df
        .filter(F.col("trip_id").isNotNull())
        .dropDuplicates(["trip_id"])
    )

    after = df.count()

    context.log.info(
        f"Validation trip_id : {before - after:,} lignes supprimées"
    )

    # -----------------------------------------------------------------------
    # 4. Cohérence temporelle
    # -----------------------------------------------------------------------

    before = df.count()

    df = (
        df
        .filter(F.col("trip_start_timestamp").isNotNull())
        .filter(F.col("trip_end_timestamp").isNotNull())
        .filter(
            F.col("trip_end_timestamp")
            >= F.col("trip_start_timestamp")
        )
    )

    after = df.count()

    context.log.info(
        f"Validation timestamps : {before - after:,} lignes supprimées"
    )

    # -----------------------------------------------------------------------
    # 5. Durée du trajet
    #
    # Le profiling montre :
    #   P99.9 ≈ 10 735 secondes
    #   maximum = 86 382 secondes
    #
    # Une limite de 6 heures permet de conserver les trajets extrêmes
    # tout en éliminant les valeurs proches de 24 heures.
    # -----------------------------------------------------------------------

    before = df.count()

    df = (
        df
        .filter(F.col("trip_seconds") >= MIN_TRIP_SECONDS)
        .filter(F.col("trip_seconds") <= MAX_TRIP_SECONDS)
    )

    after = df.count()

    context.log.info(
        f"Validation durée : {before - after:,} lignes supprimées"
    )

    # -----------------------------------------------------------------------
    # 6. Distance
    #
    # Le profiling montre :
    #   P99.9 ≈ 43.4 miles
    #   maximum = 921.1 miles
    #
    # Une limite de 100 miles permet de conserver une marge importante
    # au-dessus des valeurs observées dans la distribution normale.
    # -----------------------------------------------------------------------

    before = df.count()

    df = (
        df
        .filter(F.col("trip_miles") >= MIN_TRIP_MILES)
        .filter(F.col("trip_miles") <= MAX_TRIP_MILES)
    )

    after = df.count()

    context.log.info(
        f"Validation distance : {before - after:,} lignes supprimées"
    )

    # -----------------------------------------------------------------------
    # 7. Vitesse moyenne
    # -----------------------------------------------------------------------

    df = df.withColumn(
        "trip_speed_mph",
        F.col("trip_miles")
        / (F.col("trip_seconds") / F.lit(3600)),
    )

    before = df.count()

    df = df.filter(
        F.col("trip_speed_mph") <= MAX_SPEED_MPH
    )

    after = df.count()

    context.log.info(
        f"Validation vitesse : {before - after:,} lignes supprimées"
    )

    # -----------------------------------------------------------------------
    # 8. Montants financiers
    #
    # On conserve les montants nuls mais on élimine les valeurs négatives.
    # -----------------------------------------------------------------------

    before = df.count()

    df = (
        df
        .filter(
            F.col("fare").isNull()
            | (F.col("fare") >= 0)
        )
        .filter(
            F.col("tips").isNull()
            | (F.col("tips") >= 0)
        )
        .filter(
            F.col("tolls").isNull()
            | (F.col("tolls") >= 0)
        )
        .filter(
            F.col("extras").isNull()
            | (F.col("extras") >= 0)
        )
        .filter(
            F.col("trip_total").isNull()
            | (F.col("trip_total") >= 0)
        )
    )

    after = df.count()

    context.log.info(
        f"Validation montants : {before - after:,} lignes supprimées"
    )

    # -----------------------------------------------------------------------
    # 9. Géographie
    #
    # Les analyses Gold utilisent les zones de prise en charge et de
    # dépose. Les trajets sans ces informations ne sont donc pas conservés
    # dans la couche Silver.
    # -----------------------------------------------------------------------

    before = df.count()

    df = (
        df
        .filter(F.col("pickup_community_area").isNotNull())
        .filter(F.col("dropoff_community_area").isNotNull())
    )

    after = df.count()

    context.log.info(
        f"Validation géographique : {before - after:,} lignes supprimées"
    )

    # -----------------------------------------------------------------------
    # 10. Nombre final de lignes
    # -----------------------------------------------------------------------

    total_silver = df.count()

    context.log.info(
        f"Nombre de lignes en Silver : {total_silver:,}"
    )

    context.log.info(
        f"Nombre total supprimé : "
        f"{total_bronze - total_silver:,}"
    )

    context.log.info(
        f"Taux de conservation : "
        f"{(total_silver / total_bronze) * 100:.2f}%"
    )

    # -----------------------------------------------------------------------
    # 11. Écriture Silver
    # -----------------------------------------------------------------------

    (
        df.write
        .mode("overwrite")
        .partitionBy("trip_date")
        .parquet(SILVER_PATH)
    )

    context.log.info(
        f"Données Silver écrites dans : {SILVER_PATH}"
    )

    return None


