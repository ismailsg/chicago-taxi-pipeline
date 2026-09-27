# Résultats de la Transformation - Couche Gold

## 1. Objectif et positionnement métier

La couche **Gold** représente le niveau d'aboutissement analytique de l'architecture Medallion. Son rôle est d'exploiter les données nettoyées et certifiées de la couche **Silver** (**2 641 126 trajets**) pour produire des vues agrégées, orientées métiers et prêtes pour la consommation (tableaux de bord, requêtes SQL analytiques, BI).

Contrairement à la couche Silver axée sur l'intégrité et la qualité de donnée, la couche Gold vise l'**optimisation de la performance de lecture** et la **simplification de l'analyse** en pré-calculant les principaux indicateurs de performance (KPIs).

À partir du dataset Silver, trois tables d'agrégation distinctes ont été modélisées et matérialisées :

1. `gold_revenue_by_day`

2. `gold_avg_trip_duration`

3. `gold_top_pickup_zones`

---

## 2. Description des tables analytiques

### 2.1. Suivi des revenus quotidiens (`gold_revenue_by_day`)

Cette table regroupe les performances financières agrégées à la journée.

* **Grain :** 1 ligne = 1 jour (`trip_date`)

* **Indicateurs clés :**
  * Nombre total de trajets
  * Montant des courses (`fare`)
  * Total des pourboires (`tips`)
  * Total des péages (`tolls`) et des extras
  * Revenu global cumulé (`trip_total`)
  * Panier moyen par course (`avg_fare`)

* **Usages :** Analyse des tendances de chiffre d'affaires, saisonnalité hebdomadaire et impact des pourboires/frais annexes.

---

### 2.2. Analyse temporelle et durées (`gold_avg_trip_duration`)

Cette table mesure la fluidité et les temps de parcours quotidiens.

* **Grain :** 1 ligne = 1 jour (`trip_date`)

* **Indicateurs clés :**
  * Nombre de trajets quotidiens
  * Durée moyenne des trajets
  * **Durée médiane (P50)** et **90ᵉ percentile (P90)**
  * Distance moyenne parcourue

* **Choix technique :** L'utilisation de percentiles (P50 et P90) via la fonction PySpark `percentile_approx` offre une vision réaliste du trafic, la moyenne simple étant souvent étirée par les conditions de congestion exceptionnelles.

---

### 2.3. Sectorisation géographique (`gold_top_pickup_zones`)

Cette table évalue l'attractivité et le volume d'activité par secteur de prise en charge.

* **Grain :** 1 ligne = 1 zone géographique (`pickup_community_area`)

* **Indicateurs clés :**
  * Volume total de départs
  * Revenu total généré par zone
  * Recette moyenne par course
  * Durée et distance moyennes des trajets issus du secteur

* **Usages :** Identification des hubs d'activité (aéroports, centres-villes), comparaison de la rentabilité des zones et optimisation de l'allocation de la flotte.

---

## 3. Justification de l'architecture Gold (Modélisation multi-grain)

Plutôt que d'unifier ces métriques dans une unique table complexe, le choix s'est porté sur trois tables d'agrégation dédiées.

```
                  Silver Layer
             (2 641 126 trajets nettoyés)
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ▼                ▼                ▼
 Revenue by Day   Trip Duration    Pickup Zones
 (Grain: 1j)      (Grain: 1j)      (Grain: 1 zone)
   166 lignes       166 lignes        77 lignes
```

### Bénéfices de ce choix d'ingénierie :

* **Simplicité de requêtage :** Les outils BI (Metabase, PowerBI) interrogent directement la table adaptée au filtre demandé sans jointures coûteuses ni risque de sur-agrégation (*double-counting*).

* **Performance :** La réduction de volumétrie est massive (de 2,6M de lignes à quelques dizaines/cents de lignes), garantissant des temps de réponse instantanés pour les rapports.

---

## 4. Stockage et matérialisation

Les tables Gold sont matérialisées au format **Parquet** sur notre stockage d'objets **RustFS** :

* `s3a://gold/revenue_by_day`

* `s3a://gold/avg_trip_duration`

* `s3a://gold/top_pickup_zones`

L'exécution des transformations Spark est orchestrée par **Dagster**, garantissant le suivi des dépendances d'assets (*software-defined assets*) et la traçabilité du lineage.

---

## 5. Synthèse des résultats et périmètre temporel

La matérialisation globale via Dagster confirme les volumes finaux suivants :

| Table Gold | Grain d'agrégation | Volumétrie produite | Statut Dagster | 
 | ----- | ----- | ----- | ----- | 
| `gold_revenue_by_day` | Date (`trip_date`) | **166 jours** | ✅ Success | 
| `gold_avg_trip_duration` | Date (`trip_date`) | **166 jours** | ✅ Success | 
| `gold_top_pickup_zones` | Zone (`pickup_community_area`) | **77 zones** | ✅ Success | 

> **Note sur le périmètre temporel :**
> Les 166 jours représentés couvrent la période du **1ᵉʳ juillet au 13 décembre 2023**. Le périmètre initial visait le second semestre complet (jusqu'au 31 décembre), mais le mécanisme d'ingestion s'est arrêté à la limite fixée de 3 000 000 de lignes en Bronze lors de la journée du 13 décembre. La couche Gold reflète donc fidèlement l'intégralité de la donnée disponible.

## 6. Conclusion du pipeline

Avec la finalisation de la couche Gold, le pipeline Medallion atteint son objectif :

1. **Bronze :** Ingestion brute et traçable (3M lignes).

2. **Silver :** Filtrage et harmonisation de qualité (2,64M lignes / 88,04 % conservés).

3. **Gold :** Structuration décisionnelle prête pour l'analyse (3 tables spécialisées).