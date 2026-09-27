import os
import duckdb


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://rustfs:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "taxiadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "taxiadmin-secret")


GOLD_REVENUE = "s3://gold/revenue_by_day/*.parquet"
GOLD_DURATION = "s3://gold/avg_trip_duration/*.parquet"
GOLD_ZONES = "s3://gold/top_pickup_zones/*.parquet"


def create_connection():
    """Crée une connexion DuckDB configurée pour lire les données dans RustFS."""

    con = duckdb.connect()

    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    endpoint = (
        S3_ENDPOINT
        .replace("http://", "")
        .replace("https://", "")
    )

    con.execute(
        f"""
        CREATE SECRET rustfs (
            TYPE S3,
            KEY_ID '{S3_ACCESS_KEY}',
            SECRET '{S3_SECRET_KEY}',
            ENDPOINT '{endpoint}',
            USE_SSL false,
            URL_STYLE 'path'
        )
        """
    )

    return con


# ============================================================
# REVENUE
# ============================================================

def top_revenue_days(con):
    """
    Top 10 des jours avec le revenu total le plus élevé.

    Permet d'identifier les journées qui ont généré
    le plus de revenus sur la période analysée.
    """

    result = con.execute(
        f"""
        SELECT
            trip_date,
            nb_trips,
            total_revenue,
            avg_trip_total
        FROM read_parquet('{GOLD_REVENUE}')
        ORDER BY total_revenue DESC
        LIMIT 10
        """
    ).fetchdf()

    print("\n=== TOP 10 JOURS PAR REVENUS ===")
    print(result.to_string(index=False))


def top_trip_days(con):
    """
    Top 10 des jours avec le plus grand nombre de trajets.

    Permet d'identifier les journées avec le plus
    fort volume d'activité.
    """

    result = con.execute(
        f"""
        SELECT
            trip_date,
            nb_trips,
            total_revenue,
            avg_trip_total
        FROM read_parquet('{GOLD_REVENUE}')
        ORDER BY nb_trips DESC
        LIMIT 10
        """
    ).fetchdf()

    print("\n=== TOP 10 JOURS PAR NOMBRE DE TRAJETS ===")
    print(result.to_string(index=False))


def global_average_revenue(con):
    """
    Calcule le revenu moyen par trajet sur toute la période.

    Le calcul est pondéré par le nombre de trajets.
    On évite ainsi de faire simplement la moyenne des
    moyennes journalières.
    """

    result = con.execute(
        f"""
        SELECT
            SUM(total_revenue) AS total_revenue,
            SUM(nb_trips) AS total_trips,
            SUM(total_revenue) / SUM(nb_trips) AS avg_revenue_per_trip
        FROM read_parquet('{GOLD_REVENUE}')
        """
    ).fetchdf()

    print("\n=== REVENU MOYEN GLOBAL ===")
    print(result.to_string(index=False))


# ============================================================
# DURATION
# ============================================================

def longest_average_duration_days(con):
    """
    Top 10 des jours avec la durée moyenne de trajet la plus élevée.

    Permet d'identifier les journées pendant lesquelles
    les trajets ont été les plus longs en moyenne.
    """

    result = con.execute(
        f"""
        SELECT
            trip_date,
            nb_trips,
            avg_trip_minutes,
            median_trip_minutes,
            p90_trip_minutes
        FROM read_parquet('{GOLD_DURATION}')
        ORDER BY avg_trip_minutes DESC
        LIMIT 10
        """
    ).fetchdf()

    print("\n=== JOURS AVEC LA DURÉE MOYENNE LA PLUS ÉLEVÉE ===")
    print(result.to_string(index=False))


def highest_p90_duration_days(con):
    """
    Top 10 des jours avec le P90 de durée le plus élevé.

    Le P90 permet de regarder les trajets longs sans être
    directement influencé par les valeurs extrêmes.
    """

    result = con.execute(
        f"""
        SELECT
            trip_date,
            nb_trips,
            avg_trip_minutes,
            median_trip_minutes,
            p90_trip_minutes
        FROM read_parquet('{GOLD_DURATION}')
        ORDER BY p90_trip_minutes DESC
        LIMIT 10
        """
    ).fetchdf()

    print("\n=== JOURS AVEC LE P90 LE PLUS ÉLEVÉ ===")
    print(result.to_string(index=False))


def global_average_duration(con):
    """
    Calcule la durée moyenne d'un trajet sur toute la période.

    La moyenne est pondérée par le nombre de trajets
    de chaque journée.
    """

    result = con.execute(
        f"""
        SELECT
            SUM(avg_trip_minutes * nb_trips) / SUM(nb_trips)
                AS avg_trip_minutes,
            SUM(nb_trips) AS total_trips
        FROM read_parquet('{GOLD_DURATION}')
        """
    ).fetchdf()

    print("\n=== DURÉE MOYENNE GLOBALE ===")
    print(result.to_string(index=False))


# ============================================================
# ZONES
# ============================================================

def top_pickup_zones_by_trips(con):
    """
    Top 10 des zones de prise en charge avec le plus de trajets.

    Permet d'identifier les zones qui concentrent
    le plus grand volume de départs.
    """

    result = con.execute(
        f"""
        SELECT
            pickup_community_area,
            nb_trips,
            total_revenue,
            avg_trip_total,
            avg_trip_miles
        FROM read_parquet('{GOLD_ZONES}')
        ORDER BY nb_trips DESC
        LIMIT 10
        """
    ).fetchdf()

    print("\n=== TOP 10 ZONES PAR NOMBRE DE TRAJETS ===")
    print(result.to_string(index=False))


def top_pickup_zones_by_revenue(con):
    """
    Top 10 des zones de prise en charge par revenu total.

    Permet d'identifier les zones qui génèrent
    le plus de revenus sur la période.
    """

    result = con.execute(
        f"""
        SELECT
            pickup_community_area,
            nb_trips,
            total_revenue,
            avg_trip_total,
            avg_trip_miles
        FROM read_parquet('{GOLD_ZONES}')
        ORDER BY total_revenue DESC
        LIMIT 10
        """
    ).fetchdf()

    print("\n=== TOP 10 ZONES PAR REVENUS ===")
    print(result.to_string(index=False))


def compare_zone_activity_and_revenue(con):
    """
    Compare le volume de trajets avec le revenu moyen par trajet.

    Permet de distinguer les zones très fréquentées
    des zones où les trajets ont une valeur moyenne plus élevée.
    """

    result = con.execute(
        f"""
        SELECT
            pickup_community_area,
            nb_trips,
            total_revenue,
            avg_trip_total,
            avg_trip_seconds / 60.0 AS avg_trip_minutes,
            avg_trip_miles
        FROM read_parquet('{GOLD_ZONES}')
        ORDER BY nb_trips DESC
        LIMIT 20
        """
    ).fetchdf()

    print("\n=== COMPARAISON TRAJETS / REVENU MOYEN ===")
    print(result.to_string(index=False))


# ============================================================
# MAIN
# ============================================================

def main():
    con = create_connection()

    try:
        # Revenue
        top_revenue_days(con)
        top_trip_days(con)
        global_average_revenue(con)

        # Duration
        longest_average_duration_days(con)
        highest_p90_duration_days(con)
        global_average_duration(con)

        # Zones
        top_pickup_zones_by_trips(con)
        top_pickup_zones_by_revenue(con)
        compare_zone_activity_and_revenue(con)

    finally:
        con.close()


if __name__ == "__main__":
    main()