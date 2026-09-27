import json
from datetime import date, timedelta
import pandas as pd

import requests
import streamlit as st
import plotly.express as px

from queries import (
    create_connection,
    get_global_kpis,
    get_global_duration,
    get_revenue_by_day,
    get_top_revenue_days,
    get_top_trip_days,
    get_duration_by_day,
    get_longest_average_duration_days,
    get_highest_p90_duration_days,
    get_zones,
    get_top_zones_by_trips,
    get_top_zones_by_revenue,
    get_zone_activity_vs_revenue,
)


st.set_page_config(
    page_title="Chicago Taxi Analytics",
    page_icon="🚕",
    layout="wide",
)


# ============================================================
# ÉVÉNEMENTS À SURLIGNER SUR LES GRAPHIQUES TEMPORELS
# Ajuste les dates selon la période réelle de ton dataset.
# ============================================================

EVENTS = [
    {
        "name": "Chicago Marathon",
        "start": date(2023, 10, 8),
        "end": date(2023, 10, 8),
        "color": "rgba(220, 20, 60, 0.18)",
    },
    # Ajoute d'autres événements ici, par exemple :
    # {
    #     "name": "Lollapalooza",
    #     "start": date(2023, 8, 3),
    #     "end": date(2023, 8, 6),
    #     "color": "rgba(30, 144, 255, 0.18)",
    # },
]

# ============================================================
# LIBELLÉS LISIBLES POUR LES GRAPHIQUES ET TABLEAUX
# (les noms de colonnes techniques restent inchangés dans le code,
# seul l'affichage change)
# ============================================================

LABELS = {
    "trip_date": "Date",
    "nb_trips": "Nombre de trajets",
    "total_revenue": "Revenu total ($)",
    "avg_trip_total": "Revenu moyen par trajet ($)",
    "avg_trip_minutes": "Durée moyenne (min)",
    "median_trip_minutes": "Durée médiane (min)",
    "p90_trip_minutes": "Durée P90 (min)",
    "avg_trip_miles": "Distance moyenne (miles)",
    "avg_trip_seconds": "Durée moyenne (s)",
    "pickup_community_area": "Zone (community area)",
    "value": "Valeur",
    "zone_name": "Zone (nom)",
    "variable": "Métrique",
}


def pretty(df):
    """Retourne une copie du DataFrame avec des en-têtes de colonnes lisibles
    et les dates tronquées à YYYY-MM-DD, pour l'affichage uniquement."""
    df = df.copy()
    if "trip_date" in df.columns:
        df["trip_date"] = pd.to_datetime(df["trip_date"]).dt.strftime("%Y-%m-%d")
    return df.rename(columns=LABELS)


CHICAGO_GEOJSON_URL = "https://data.cityofchicago.org/resource/igwz-8jzy.geojson"

# Nom de la propriété GeoJSON qui contient le numéro de community area.
# Vérifié une fois via st.write(geojson["features"][0]["properties"]) —
# ajuste cette constante si le nom diffère au moment où tu testes.
GEOJSON_AREA_KEY = "properties.area_num_1"


def add_event_bands(fig, events, date_min=None, date_max=None):
    """Superpose des bandes colorées pour chaque événement présent
    dans la plage de dates actuellement affichée sur le graphique."""
    for ev in events:
        start, end = ev["start"], ev["end"]

        if date_min is not None and end < date_min:
            continue
        if date_max is not None and start > date_max:
            continue

        # élargit d'un jour les événements ponctuels pour qu'ils restent visibles
        x1 = end + timedelta(days=1) if start == end else end

        fig.add_vrect(
            x0=start,
            x1=x1,
            fillcolor=ev["color"],
            line_width=0,
            annotation_text=ev["name"],
            annotation_position="top left",
            annotation_font_size=11,
        )
    return fig


# ============================================================
# CONNEXION
# ============================================================

@st.cache_resource
def get_connection():
    """Garde une connexion DuckDB réutilisable pendant la session."""
    return create_connection()


con = get_connection()


@st.cache_data(ttl=3600)
def load_chicago_geojson():
    """Contours des community areas de Chicago (source: data.cityofchicago.org)."""
    resp = requests.get(CHICAGO_GEOJSON_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()

def get_area_to_name():
    """Correspondance {numéro de community area (str) -> nom de la zone},
    construite à partir du GeoJSON. Retourne un dict vide si le GeoJSON
    est indisponible, pour ne pas casser le reste de l'app."""
    try:
        geojson = load_chicago_geojson()
        return {
            feat["properties"]["area_num_1"]: feat["properties"]["community"].title()
            for feat in geojson["features"]
        }
    except Exception:
        return {}


def add_zone_names(df):
    """Ajoute une colonne 'zone_name' juste après 'pickup_community_area'."""
    area_to_name = get_area_to_name()
    df = df.copy()
    df["zone_name"] = (
        df["pickup_community_area"].astype(int).astype(str).map(area_to_name)
    )
    cols = list(df.columns)
    cols.insert(cols.index("pickup_community_area") + 1, cols.pop(cols.index("zone_name")))
    return df[cols]

# ============================================================
# CHARGEMENT DES DONNÉES DE BASE (mis en cache)
# ============================================================

@st.cache_data(ttl=600)
def load_kpis(_con):
    return get_global_kpis(_con), get_global_duration(_con)


@st.cache_data(ttl=600)
def load_revenue_full(_con):
    """Chargée une fois pour déterminer les bornes de dates disponibles."""
    return get_revenue_by_day(_con)


try:
    kpis, duration = load_kpis(con)
    revenue_full = load_revenue_full(con)
except Exception as e:
    st.error(
        "Impossible de charger les données gold. "
        "Le pipeline a-t-il bien été matérialisé jusqu'à la couche gold ?"
    )
    st.exception(e)
    st.stop()

if revenue_full.empty:
    st.warning("Aucune donnée disponible pour le moment.")
    st.stop()


# ============================================================
# HEADER + KPIs
# ============================================================

st.title("🚕 Chicago Taxi Analytics")
st.caption("Trajets de taxis de Chicago — couches Bronze / Silver / Gold")

total_trips = int(kpis.iloc[0]["total_trips"])
total_revenue = float(kpis.iloc[0]["total_revenue"])
avg_revenue = float(kpis.iloc[0]["avg_revenue_per_trip"])
avg_duration = float(duration.iloc[0]["avg_trip_minutes"])

col1, col2, col3, col4 = st.columns(4)
col1.metric("Nombre de trajets", f"{total_trips:,.0f}")
col2.metric("Revenu total", f"${total_revenue:,.0f}")
col3.metric("Revenu moyen / trajet", f"${avg_revenue:,.2f}")
col4.metric("Durée moyenne", f"{avg_duration:.1f} min")

st.divider()


# ============================================================
# FILTRES (sidebar)
# ============================================================

with st.sidebar:
    st.header("Filtres")

    date_min = revenue_full["trip_date"].min()
    date_max = revenue_full["trip_date"].max()

    date_range = st.date_input(
        "Période",
        value=(date_min, date_max),
        min_value=date_min,
        max_value=date_max,
    )

    top_n = st.slider("Nombre d'éléments affichés (tops, zones)", 5, 30, 10)

    show_events = st.checkbox("Afficher les événements sur les graphiques", value=True)

if len(date_range) == 2:
    start, end = date_range
else:
    start, end = date_min, date_max

active_events = EVENTS if show_events else []


# ============================================================
# CHARGEMENT DES DONNÉES FILTRÉES / DÉPENDANTES DES FILTRES
# ============================================================

@st.cache_data(ttl=600)
def load_revenue(_con, start, end):
    return get_revenue_by_day(_con, start=start, end=end)


@st.cache_data(ttl=600)
def load_duration(_con, start, end):
    return get_duration_by_day(_con, start=start, end=end)


@st.cache_data(ttl=600)
def load_top_revenue_days(_con, limit):
    return get_top_revenue_days(_con, limit=limit)


@st.cache_data(ttl=600)
def load_top_trip_days(_con, limit):
    return get_top_trip_days(_con, limit=limit)


@st.cache_data(ttl=600)
def load_longest_duration_days(_con, limit):
    return get_longest_average_duration_days(_con, limit=limit)


@st.cache_data(ttl=600)
def load_highest_p90_days(_con, limit):
    return get_highest_p90_duration_days(_con, limit=limit)


@st.cache_data(ttl=600)
def load_zones_full(_con):
    return get_zones(_con)


@st.cache_data(ttl=600)
def load_top_zones(_con, limit):
    return get_top_zones_by_trips(_con, limit=limit), get_top_zones_by_revenue(_con, limit=limit)


@st.cache_data(ttl=600)
def load_zone_comparison(_con):
    return get_zone_activity_vs_revenue(_con)


revenue_filtered = load_revenue(con, start, end)
duration_filtered = load_duration(con, start, end)


# ============================================================
# ONGLETS
# ============================================================

tab_revenue, tab_duration, tab_zones, tab_raw = st.tabs(
    ["Revenus & activité", "Durée des trajets", "Zones & carte", "Données brutes"]
)

with tab_revenue:
    col1, col2 = st.columns(2)

    with col1:
        fig = px.line(
            revenue_filtered, x="trip_date", y="nb_trips",
            title="Nombre de trajets par jour",
            labels=LABELS,
        )
        fig.update_xaxes(tickformat="%Y-%m-%d")
        add_event_bands(fig, active_events, date_min=start, date_max=end)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.line(
            revenue_filtered, x="trip_date", y="total_revenue",
            title="Revenus par jour",
            labels=LABELS,
        )
        fig.update_yaxes(tickprefix="$")
        fig.update_xaxes(tickformat="%Y-%m-%d")
        add_event_bands(fig, active_events, date_min=start, date_max=end)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader(f"Top {top_n} jours")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Par revenus**")
        st.dataframe(
            pretty(load_top_revenue_days(con, top_n)),
            use_container_width=True,
            hide_index=True,
        )

    with col2:
        st.markdown("**Par nombre de trajets**")
        st.dataframe(
            pretty(load_top_trip_days(con, top_n)),
            use_container_width=True,
            hide_index=True,
        )

    st.download_button(
        "Télécharger les revenus par jour (CSV)",
        revenue_filtered.to_csv(index=False).encode("utf-8"),
        "revenue_by_day.csv",
        "text/csv",
    )

with tab_duration:
    fig = px.line(
        duration_filtered,
        x="trip_date",
        y=["avg_trip_minutes", "median_trip_minutes", "p90_trip_minutes"],
        title="Évolution de la durée des trajets",
        labels=LABELS,
    )
    fig.update_xaxes(tickformat="%Y-%m-%d")
    add_event_bands(fig, active_events, date_min=start, date_max=end)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader(f"Top {top_n} jours")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Durée moyenne la plus longue**")
        st.dataframe(
            pretty(load_longest_duration_days(con, top_n)),
            use_container_width=True,
            hide_index=True,
        )

    with col2:
        st.markdown("**P90 de durée le plus élevé**")
        st.dataframe(
            pretty(load_highest_p90_days(con, top_n)),
            use_container_width=True,
            hide_index=True,
        )

with tab_zones:
    zones_top, zones_revenue_top = load_top_zones(con, top_n)
    zone_comparison = load_zone_comparison(con)

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(
            zones_top, x="pickup_community_area", y="nb_trips",
            title=f"Top {top_n} zones par nombre de trajets",
            labels=LABELS,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.bar(
            zones_revenue_top, x="pickup_community_area", y="total_revenue",
            title=f"Top {top_n} zones par revenus",
            labels=LABELS,
        )
        fig.update_yaxes(tickprefix="$")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Nombre de trajets vs revenu moyen")
    fig = px.scatter(
        zone_comparison,
        x="nb_trips",
        y="avg_trip_total",
        size="total_revenue",
        hover_name="pickup_community_area",
        title="Activité vs rentabilité moyenne par zone",
        labels=LABELS,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Carte des zones")
 

    try:
        geojson = load_chicago_geojson()
        zones_map_df = load_zones_full(con).copy()
        zones_map_df["pickup_community_area"] = (
            zones_map_df["pickup_community_area"].astype(int).astype(str)
        )

        map_col1, map_col2 = st.columns(2)

        with map_col1:
            fig = px.choropleth(
                zones_map_df,
                geojson=geojson,
                locations="pickup_community_area",
                featureidkey=GEOJSON_AREA_KEY,
                color="nb_trips",
                color_continuous_scale="Oranges",
                hover_data=["total_revenue", "avg_trip_total"],
                labels=LABELS,
                title="Zones les plus fréquentées",
            )
            fig.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig, use_container_width=True)

        with map_col2:
            fig = px.choropleth(
                zones_map_df,
                geojson=geojson,
                locations="pickup_community_area",
                featureidkey=GEOJSON_AREA_KEY,
                color="total_revenue",
                color_continuous_scale="Greens",
                hover_data=["nb_trips", "avg_trip_total"],
                labels=LABELS,
                title="Zones les plus rentables",
            )
            fig.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig, use_container_width=True)

        # with st.expander("Debug : clés disponibles dans le GeoJSON"):
        #     st.write(geojson["features"][0]["properties"])

    except Exception as e:
        st.warning("Impossible de charger la carte des zones.")
        st.exception(e)

    st.subheader("Détail des zones")
    st.dataframe(
        pretty(
            add_zone_names(
            zone_comparison[
                [
                    "pickup_community_area", "nb_trips", "total_revenue",
                    "avg_trip_total", "avg_trip_minutes", "avg_trip_miles",
                ]
            ]
            )
        ),
        use_container_width=True,
        hide_index=True,
    )

with tab_raw:
    st.subheader("Aperçu des tables gold")
    st.markdown("**Revenus par jour**")
    st.dataframe(pretty(revenue_full), use_container_width=True, hide_index=True)
    st.markdown("**Durée des trajets par jour**")
    st.dataframe(pretty(load_duration(con, date_min, date_max)), use_container_width=True, hide_index=True)
    st.markdown("**Activité par zone**")
    st.dataframe(pretty(add_zone_names(load_zones_full(con))), use_container_width=True, hide_index=True)
    st.dataframe(pretty(load_zones_full(con)), use_container_width=True, hide_index=True)