# Roadmap : prochaines évolutions du pipeline

Ce pipeline (Bronze, Silver, Gold, orchestré avec Dagster et PySpark) est livré et fonctionnel. Ce document décrit les améliorations que je prévois dans les prochains jours pour le faire passer d'un pipeline qui marche à un pipeline prêt pour la production.

## Ce qui est en place

- **Architecture médaillon** orchestrée avec des assets Dagster (`bronze_taxi_trips`, `silver_taxi_trips`) et un lineage clair.
- **Seuils de nettoyage issus du profiling**, pas choisis au hasard. Exemple : durée plafonnée à 6 h (P99.9 à environ 10 735 s, maximum à 86 382 s), distance plafonnée à 100 miles (P99.9 à environ 43,4 miles, maximum à 921,1 miles).
- **Typage explicite**, dédoublonnage sur `trip_id`, validation des timestamps, des montants et des zones géographiques.
- **Journalisation du nombre de lignes supprimées par règle**, pour garder une visibilité sur chaque étape de nettoyage.
- **Notes d'analyse sur les valeurs aberrantes** et tableau de bord pour l'exploration.
- **Écriture partitionnée par `trip_date`** pour préparer les lectures analytiques.

## Ce que je vais ajouter

Je classe les évolutions par impact. Chacune s'appuie sur l'existant, rien n'est à refaire de zéro.

### 1. Performance : une seule passe sur les données
Aujourd'hui, chaque règle de Silver mesure son effet avec un `count()` avant/après. Comme Spark évalue paresseusement, chaque `count()` rejoue le plan depuis Bronze. Je vais remplacer ces comptes par une colonne `reject_reason` calculée en une fois, un `persist()` du DataFrame et un seul `groupBy` pour toutes les métriques.

### 2. Auditabilité : une table de quarantaine
Les compteurs disent combien de lignes sont écartées. Je vais aussi conserver lesquelles et pourquoi, dans une table `quarantine` partitionnée par motif de rejet. Cela permet l'audit, le rejeu et la réconciliation `bronze = silver + quarantaine`.

### 3. Fiabilité : tests et contrôles qualité
- Règles de nettoyage extraites en fonctions `DataFrame -> DataFrame`, testées avec `pytest` aux frontières des seuils (21 600 s conservé, 21 601 s rejeté, 100 miles conservé, etc.).
- **Asset checks Dagster** : Silver non vide, taux de rétention dans une plage attendue, absence de doublons sur `trip_id`, absence de montants négatifs.
- Gardes explicites (échec clair si Bronze ou Silver est vide).

### 4. Robustesse de l'écriture
- Écrasement dynamique des partitions (`partitionOverwriteMode=dynamic`) pour des reruns partiels sans tout réécrire.
- Relecture après écriture pour vérifier que le nombre de lignes écrites correspond au nombre attendu.

### 5. Configuration plutôt que constantes
Les seuils (durée, distance, vitesse) passeront dans une `Config` Dagster avec les valeurs du profiling comme défauts, modifiables depuis l'UI sans toucher au code.

### 6. Observabilité
Les métriques de chaque run (lignes lues, conservées, rejetées par motif, taux de rétention) seront publiées en métadonnées Dagster pour suivre leur évolution d'un run à l'autre.

### 7. Pistes de plus long terme
- Déduplication déterministe avec une fenêtre (`row_number`) et un critère explicite.
- Contrat de schéma vérifié à la lecture de Bronze.
- Format transactionnel (Delta Lake ou Iceberg) pour des écritures atomiques.
- Intégration continue (tests et lint sur chaque push).

## Pourquoi cet ordre

Les points 1 à 3 apportent le plus : meilleures performances, traçabilité et filet de sécurité contre les régressions. Les points 4 à 6 durcissent l'exploitation. Le point 7 prépare le passage à l'échelle.

## Suivi

Chaque évolution sera ajoutée dans une branche dédiée avec ses tests, et cochée ici une fois fusionnée.

- [ ] Tag de rejet et `persist` (1)
- [ ] Table de quarantaine et réconciliation (2)
- [ ] Tests de frontières et asset checks (3)
- [ ] Écriture dynamique et relecture (4)
- [ ] Configuration Dagster des seuils (5)
- [ ] Métadonnées Dagster (6)
