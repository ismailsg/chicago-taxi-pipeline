# Résultats de la Transformation - Couche Silver

## 1. Objectif et périmètre du traitement

La couche **Silver** a pour rôle de transformer les données brutes issues de la couche **Bronze** en un dataset nettoyé, structuré et pleinement exploitable pour les analyses agrégées de la couche **Gold**.

Le job de nettoyage a été orchestré par **Dagster** et exécuté avec **Apache Spark**, en traitant l'intégralité du snapshot Bronze composé de **3 000 000 de trajets**.

---

## 2. Contrôles de qualité appliqués

Chaque trajet a été soumis à une série de filtres et de contrôles de cohérence basés sur les règles validées lors de l'analyse exploratoire :

* **Identifiants (`trip_id`) :** Vérification de la présence des clés et suppression des doublons éventuels.
* **Timestamps :** Contrôle de la cohérence temporelle (`trip_start_timestamp` et `trip_end_timestamp`).
* **Durée (`trip_seconds`) :** Élimination des durées nulles ainsi que des sessions anormalement longues (> 6 heures).
* **Distance (`trip_miles`) :** Plafonnement à 100 miles maximum.
* **Vitesse moyenne calculée :** Recalcul de la vitesse et éviction des valeurs supérieures à 100 mph.
* **Montants financiers :** Validation de la non-négativité des tarifs, pourboires, péages et frais annexes.
* **Secteurs géographiques :** Présence obligatoire des zones de prise en charge et de dépose (`pickup_community_area` et `dropoff_community_area`).

---

## 3. Bilan de la filtration

L'application combinée des règles de qualité a produit le bilan de suppression suivant :

| Dimension du contrôle | Lignes supprimées | Part des rejets (%) |
| --- | ---: | ---: |
| Identifiant (`trip_id` nul ou doublon) | 0 | 0,00 % |
| Timestamps (incohérents ou manquants) | 88 | ~0,02 % |
| Durée (0s ou > 6h) | 61 631 | ~17,17 % |
| Distance (> 100 miles) | 265 | ~0,07 % |
| Vitesse calculée (> 100 mph) | 1 516 | ~0,42 % |
| Montants financiers (< 0) | 0 | 0,00 % |
| Géographie (zones de départ/arrivée absentes) | 295 374 | ~82,31 % |
| **Total des lignes rejetées** | **358 874** | **100,00 %** |

### Synthèse des résultats :

* **Volume initial (Bronze) :** 3 000 000 trajets
* **Volume conservé (Silver) :** 2 641 126 trajets
* **Taux de conservation :** **88,04 %** (soit environ 11,96 % de données exclues)

### Analyse des rejets :

1. **Information géographique (cause principale) :** La majorité des suppressions (295 374 lignes) s'explique par l'absence d'identifiant de zone de départ ou d'arrivée. Étant donné que les besoins analytiques de la couche Gold reposent sur les flux inter-quartiers, conserver ces lignes sans affectation territoriale aurait faussé les agrégations.
2. **Coordonnées de durée et de vitesse :** 61 631 trajets ont été écartés principalement en raison de durées nulles, un comportement typique des compteurs annulés prématurément par le conducteur.
3. **Piste financière :** L'absence totale de valeurs négatives indique une bonne intégrité initiale sur les données tarifaires.

---

## 4. Stockage et découpage

Les données nettoyées et validées de la couche Silver sont enregistrées au format orienté colonne **Parquet** sur notre stockage d'objets **RustFS** :

* **URI de destination :** `s3a://silver/taxi_trips_clean`
* **Partitionnement :** Découpé par date de trajet (`trip_date`).

Ce partitionnement garantit des requêtes efficaces et ciblées pour les futurs calculs quotidiens ou mensuels de la couche Gold.

---

## 5. Synthèse du flux de données

```text
Bronze (3 000 000 trajets)
  │
  ▼
Nettoyage & Validation PySpark / Dagster
  │
  ├── 88 timestamps invalides
  ├── 61 631 durées aberrantes (0s ou > 6h)
  ├── 265 distances hors bornes (> 100 mi)
  ├── 1 516 vitesses incohérentes (> 100 mph)
  └── 295 374 trajets sans sectorisation géographique
  │
  ▼
Silver (2 641 126 trajets consolidés - 88,04 %)
  │
  ▼
Stockage Parquet partitionné sur RustFS
(s3a://silver/taxi_trips_clean)
```

La couche Silver est désormais finalisée et fournit un socle sain et certifié pour alimenter les tables d'agrégation métiers de la couche **Gold**.