# Analyse exploratoire des valeurs aberrantes

## 1. Objectif et démarche de l'analyse

Dans le cadre du pipeline de données Taxi (medallion architecture), avant de mettre en place les transformations et filtres de nettoyage de la couche **Silver**, j'ai réalisé une analyse exploratoire approfondie sur les données brutes issues de la couche **Bronze**.

L'objectif n'est pas de supprimer aveuglément les valeurs extrêmes (*outliers*), mais de comprendre la distribution des variables clés d'un trajet pour distinguer les cas d'usage marginaux réels des anomalies techniques ou erreurs de saisie. Cette démarche permet de **justifier quantitativement chaque règle de filtrage** appliquées dans le code de production (`silver.py`), évitant ainsi d'imposer des seuils arbitraires.

Les variables principales étudiées sont :
* La durée du trajet (`trip_seconds`)
* La distance du trajet (`trip_miles`)
* La vitesse moyenne recalculée
* L'unicité des identifiants (`trip_id`)
* La couverture géographique (zones et GPS)
* La cohérence de la grille tarifaire

L'analyse porte sur un snapshot de **3 000 000 de trajets**.

### Modalités d'exécution du profiling

Cette analyse a été réalisée directement sur l'environnement conteneurisé du projet via le script dédié `scripts/profile_taxi_data.py`. L'exécution a été lancée dans le conteneur `dagster-webserver` pour bénéficier du contexte d'exécution et des accès aux volumes de données Bronze avec la commande suivante :

```bash
docker compose run --rm \
  --entrypoint python \
  dagster-webserver \
  /opt/dagster/scripts/profile_taxi_data.py

---

## 2. Validation des identifiants (`trip_id`)

Les métriques d'intégrité sur la clé primaire sont les suivantes :

| Indicateur | Valeur observée |
| :--- | ---: |
| Nombre total de lignes | 3 000 000 |
| Nombre de `trip_id` uniques | 3 000 000 |
| Nombre de `trip_id` nuls | 0 |

**Constat :** Le dataset analysé ne présente aucun doublon sur la clé de trajet.

**Décision d'architecture :** Bien qu'aucun doublon n'ait été détecté sur cet échantillon, la logique de déduplication (`dropDuplicates(['trip_id'])`) est maintenue dans le script de transformation Silver. Cela garantit l'idempotence et la robustesse du pipeline face à d'éventuels rechargements ou duplications lors d'ingestions futures.

---

## 3. Distribution et analyse de la durée (`trip_seconds`)

L'étude des percentiles de la durée donne les résultats suivants :

| Statistique / Percentile | Valeur |
| :--- | ---: |
| Minimum | 0 s |
| **P50 (Médiane)** | **953 s (~16 min)** |
| P90 | 2 616 s (~43 min) |
| P95 | 3 142 s (~52 min) |
| P99 | 4 380 s (~1h13) |
| P99.5 | 5 100 s (~1h25) |
| P99.9 | 10 735 s (~2h58) |
| **Maximum** | **86 382 s (~23h59)** |

**Constat :** 
* La médiane s'établit autour de 16 minutes, ce qui reflète parfaitement la réalité d'un trajet urbain classique.
* 99,9 % des trajets durent moins de 3 heures.
* Les valeurs extrêmes atteignent près de **24 heures** (86 382 s), ce qui correspond généralement à des compteurs non arrêtés ou des pannes du système d'horodatage.
* À l'inverse, des durées de **0 seconde** sont observées sur des trajets enregistrant pourtant une distance non nulle.

### Règle métier retenue (Silver) :

```python
MIN_TRIP_SECONDS = 1
MAX_TRIP_SECONDS = 6 * 3600  # 6 heures
```

*Le seuil maximal de 6 heures offre une marge de sécurité confortable au-delà du P99.9 (3h) tout en éliminant les sessions aberrantes de près d'une journée.*

---

## 4. Distribution et analyse de la distance (`trip_miles`)

L'étude de la distance parcourue révèle la distribution suivante :

| Statistique / Percentile | Valeur |
| :--- | ---: |
| Minimum | 0,0 mile |
| **P50 (Médiane)** | **2,9 miles** |
| P90 | 17,55 miles |
| P95 | 18,41 miles |
| P99 | 26,75 miles |
| P99.5 | 30,90 miles |
| P99.9 | 43,40 miles |
| **Maximum** | **921,10 miles** |

L'analyse croisée des cas extrêmes montre des anomalies flagrantes :
* **921,1 miles** parcourus en **0 seconde**
* **913,1 miles** parcourus en **0 seconde**
* **881,5 miles** parcourus en **60 secondes** (soit > 52 000 mph)

### Règle métier retenue (Silver) :

```python
MIN_TRIP_MILES = 0.0
MAX_TRIP_MILES = 100.0
```

*Un plafond de 100 miles couvre largement le P99.9 (43,4 miles) pour capturer les trajets exceptionnels vers des aéroports lointains ou des banlieues éloignées, tout en écartant les aberrations physiques. Les distances nulles sont tolérées car elles peuvent correspondre à une annulation immédiate avec prise en charge minimale.*

---

## 5. Analyse de la vitesse moyenne

La vitesse moyenne est une variable dérivée calculée par la formule :

$$\text{Vitesse (mph)} = \frac{\text{Distance (miles)}}{\text{Durée (heures)}} = \frac{\text{trip\_miles}}{\text{trip\_seconds} / 3600}$$

| Statistique / Percentile | Valeur |
| :--- | ---: |
| Minimum | 0,00 mph |
| **P50 (Médiane)** | **13,28 mph** |
| P90 | 34,20 mph |
| P95 | 40,51 mph |
| P99 | 50,76 mph |
| P99.9 | 67,41 mph |
| **Maximum** | **101 520,00 mph** |

**Constat :** La médiane de ~13 mph reflète la congestion urbaine standard. Le croisement des erreurs sur les variables de durée et de distance génère des vitesses calculées physiquement impossibles (ex: > 100 000 mph). Le contrôle de la vitesse est un filtre croisé indispensable car il identifie les incohérences combinées qu'aucune variable prise isolément ne permet de détecter.

### Règle métier retenue (Silver) :

```python
MAX_SPEED_MPH = 100.0
```

*Ce seuil laisse une marge suffisante pour les portions d'autoroute dégagées (P99.9 à ~67 mph) tout en éliminant les incohérences de calcul.*

---

## 6. Intégrité des données financières

Les variables monétaires étudiées sont : `fare` (course), `tips` (pourboire), `tolls` (péages), `extras` et `trip_total`.

**Constat :** Les valeurs négatives sur les montants traduisent généralement des erreurs de saisie ou des rejets de paiement non régularisés dans la source.

### Règle métier retenue (Silver) :

```sql
fare >= 0 AND tips >= 0 AND tolls >= 0 AND extras >= 0 AND trip_total >= 0
```

*Les montants égaux à zéro restent autorisés (par exemple un trajet sans pourboire ou sans péage).*

---

## 7. Cohérence des données géographiques

Les analyses métier de la couche Gold reposent principalement sur les zones d'origine et de destination (`pickup_community_area`, `dropoff_community_area`).

### Règle métier retenue (Silver) :
* **Rejet** des lignes où `pickup_community_area` ou `dropoff_community_area` est nul (champs requis pour les agrégations sectorielles).
* **Tolérance** sur l'absence de coordonnées GPS exactes (`latitude`/`longitude`) : un trajet sans GPS reste exploitable pour le suivi des volumes d'activité, la durée moyenne et le chiffre d'affaires par zone.

---

## 8. Synthèse des règles de qualité (Filtres Silver)

En résumé, l'ensemble des règles de validation appliquées dans la couche Silver s'établit ainsi :

```python
# Seuils quantitatifs issus du profiling
1 <= trip_seconds <= 21600         # Durée entre 1s et 6h
0.0 <= trip_miles <= 100.0         # Distance <= 100 miles
calculated_speed_mph <= 100.0      # Vitesse <= 100 mph

# Intégrité référentielle et temporelle
trip_id IS NOT NULL
trip_start_timestamp IS NOT NULL
trip_end_timestamp IS NOT NULL
trip_end_timestamp >= trip_start_timestamp

# Informations sectorielles & financières
pickup_community_area IS NOT NULL
dropoff_community_area IS NOT NULL
fare >= 0 AND tips >= 0 AND tolls >= 0 AND extras >= 0 AND trip_total >= 0
```

---

## 9. Pourquoi ce document dans le projet ?

Ce document sert de **référentiel de justification métier et technique**. Dans une architecture data de production, le code applicatif (`silver.py`) ne doit contenir que la logique d'exécution. Expliciter la provenance des constantes et des seuils dans un document dédié apporte plusieurs bénéfices :

1. **Traçabilité :** Prouve que les filtres reposent sur l'observation réelle des données et non sur des présomptions.
2. **Maintenabilité :** Si le volume ou le comportement des trajets évolue, ce document sert de base pour comparer de nouvelles distributions et réajuster les seuils.
3. **Clarté du code :** Le script PySpark / Dagster reste sobre et lisible, focalisé sur la transformation.

---

## 10. Conclusion

L'analyse exploratoire démontre la grande qualité globale du dataset (99 % des lignes sont parfaitement cohérentes), mais confirme la présence d'anomalies extrêmes susceptibles de fausser gravement les rapports de la couche **Gold** (moyennes de vitesse, revenus totaux, durées moyennes).

Les filtres définis sécurisent la qualité des données analytiques finales sans sur-filtrer les trajets légitimes mais atypiques.