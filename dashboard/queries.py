import os
import duckdb


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://rustfs:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "taxiadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "taxiadmin-secret")

GOLD_REVENUE = "s3://gold/revenue_by_day/*.parquet"
GOLD_DURATION = "s3://gold/avg_trip_duration/*.parquet"
GOLD_ZONES = "s3://gold/top_pickup_zones/*.parquet"


def create_connection():
    """Crée une connexion DuckDB configurée pour lire les Gold dans RustFS."""

    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    endpoint = S3_ENDPOINT.replace("http://", "").replace("https://", "")

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


def _read_gold(path: str) -> str:
    """Fragment SQL réutilisable pour lire une table gold donnée."""
    return f"read_parquet('{path}')"


# ============================================================
# KPIs
# ============================================================

def get_global_kpis(con):
    """Indicateurs globaux affichés dans les cartes KPI du dashboard."""
    query = f"""
        SELECT
            SUM(nb_trips) AS total_trips,
            SUM(total_revenue) AS total_revenue,
            SUM(total_revenue) / SUM(nb_trips) AS avg_revenue_per_trip
        FROM {_read_gold(GOLD_REVENUE)}
    """
    return con.execute(query).fetchdf()


def get_global_duration(con):
    """Durée moyenne globale des trajets, pondérée par le nombre de trajets/jour."""
    query = f"""
        SELECT
            SUM(avg_trip_minutes * nb_trips) / SUM(nb_trips) AS avg_trip_minutes
        FROM {_read_gold(GOLD_DURATION)}
    """
    return con.execute(query).fetchdf()


# ============================================================
# REVENUE
# ============================================================

def get_revenue_by_day(con, start=None, end=None):
    """
    Revenus et nombre de trajets par jour.
    Si start/end sont fournis (objets date), filtre la période en SQL.
    """
    where_clause = ""
    params = []

    if start is not None and end is not None:
        where_clause = "WHERE trip_date BETWEEN ? AND ?"
        params = [start, end]

    query = f"""
        SELECT trip_date, nb_trips, total_revenue, avg_trip_total
        FROM {_read_gold(GOLD_REVENUE)}
        {where_clause}
        ORDER BY trip_date
    """
    return con.execute(query, params).fetchdf()


def get_top_revenue_days(con, limit=10):
    """Jours avec les revenus les plus élevés."""
    query = f"""
        SELECT trip_date, nb_trips, total_revenue, avg_trip_total
        FROM {_read_gold(GOLD_REVENUE)}
        ORDER BY total_revenue DESC
        LIMIT ?
    """
    return con.execute(query, [limit]).fetchdf()


def get_top_trip_days(con, limit=10):
    """Jours avec le plus grand nombre de trajets."""
    query = f"""
        SELECT trip_date, nb_trips, total_revenue, avg_trip_total
        FROM {_read_gold(GOLD_REVENUE)}
        ORDER BY nb_trips DESC
        LIMIT ?
    """
    return con.execute(query, [limit]).fetchdf()


# ============================================================
# DURATION
# ============================================================

def get_duration_by_day(con, start=None, end=None):
    """Indicateurs de durée par jour, filtrables sur une période."""
    where_clause = ""
    params = []

    if start is not None and end is not None:
        where_clause = "WHERE trip_date BETWEEN ? AND ?"
        params = [start, end]

    query = f"""
        SELECT trip_date, nb_trips, avg_trip_minutes,
               median_trip_minutes, p90_trip_minutes
        FROM {_read_gold(GOLD_DURATION)}
        {where_clause}
        ORDER BY trip_date
    """
    return con.execute(query, params).fetchdf()


def get_longest_average_duration_days(con, limit=10):
    """Jours avec la durée moyenne la plus élevée."""
    query = f"""
        SELECT trip_date, nb_trips, avg_trip_minutes,
               median_trip_minutes, p90_trip_minutes
        FROM {_read_gold(GOLD_DURATION)}
        ORDER BY avg_trip_minutes DESC
        LIMIT ?
    """
    return con.execute(query, [limit]).fetchdf()


def get_highest_p90_duration_days(con, limit=10):
    """Jours avec le P90 de durée le plus élevé."""
    query = f"""
        SELECT trip_date, nb_trips, avg_trip_minutes,
               median_trip_minutes, p90_trip_minutes
        FROM {_read_gold(GOLD_DURATION)}
        ORDER BY p90_trip_minutes DESC
        LIMIT ?
    """
    return con.execute(query, [limit]).fetchdf()


# ============================================================
# ZONES
# ============================================================

def get_zones(con):
    """Indicateurs disponibles pour chaque zone (table complète, non triée)."""
    query = f"""
        SELECT pickup_community_area, nb_trips, total_revenue,
               avg_trip_total, avg_trip_seconds / 60.0 AS avg_trip_minutes,
               avg_trip_miles
        FROM {_read_gold(GOLD_ZONES)}
    """
    return con.execute(query).fetchdf()


def get_top_zones_by_trips(con, limit=10):
    """Zones avec le plus grand nombre de trajets."""
    query = f"""
        SELECT pickup_community_area, nb_trips, total_revenue,
               avg_trip_total, avg_trip_miles
        FROM {_read_gold(GOLD_ZONES)}
        ORDER BY nb_trips DESC
        LIMIT ?
    """
    return con.execute(query, [limit]).fetchdf()


def get_top_zones_by_revenue(con, limit=10):
    """Zones avec les revenus les plus élevés."""
    query = f"""
        SELECT pickup_community_area, nb_trips, total_revenue,
               avg_trip_total, avg_trip_miles
        FROM {_read_gold(GOLD_ZONES)}
        ORDER BY total_revenue DESC
        LIMIT ?
    """
    return con.execute(query, [limit]).fetchdf()


def get_zone_activity_vs_revenue(con):
    """Volume de trajets vs revenu moyen par trajet, pour chaque zone."""
    query = f"""
        SELECT pickup_community_area, nb_trips, total_revenue,
               avg_trip_total, avg_trip_seconds / 60.0 AS avg_trip_minutes,
               avg_trip_miles
        FROM {_read_gold(GOLD_ZONES)}
        ORDER BY nb_trips DESC
    """
    return con.execute(query).fetchdf()