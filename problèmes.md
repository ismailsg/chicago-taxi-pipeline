# Problemes rencontres et resolutions

Ce fichier rassemble les problemes rencontres pendant le developpement du pipeline,
ainsi que la demarche suivie pour les comprendre et les resoudre.

L'objectif n'est pas seulement de noter la solution finale. Je veux aussi garder
une trace des erreurs observees, des verifications effectuees et de ce que j'en
retiendrai pour la suite.

## 1. Incompatibilite de versions avec PostgreSQL et Dagster

### Contexte

Le projet utilise Dagster avec PostgreSQL pour stocker les informations de
l'orchestrateur. Les dependances principales etaient notamment :

- `dagster==1.8.12`
- `dagster-postgres==0.24.12`
- SQLAlchemy

Apres le passage a RustFS, les conteneurs RustFS et l'initialisation des buckets
fonctionnaient correctement. Pourtant, les conteneurs Dagster s'arretaient.

### Probleme observe

Le premier message d'erreur etait :

```text
ModuleNotFoundError: No module named 'psycopg'
```

J'ai d'abord ajoute `psycopg`, le driver PostgreSQL plus recent. Le build Docker
passait alors, mais Dagster rencontrait ensuite une erreur de connexion :

```text
psycopg.OperationalError: the connection is closed
```

La premiere erreur indiquait donc qu'un driver manquait, mais elle ne donnait pas
encore la vraie solution.

### Demarche de resolution

J'ai suivi plusieurs etapes :

1. J'ai consulte les logs des conteneurs `dagster-webserver` et
   `dagster-daemon`.
2. J'ai verifie que PostgreSQL etait bien demarre et en bonne sante.
3. J'ai teste la connexion a PostgreSQL depuis une image Python.
4. J'ai regarde les dependances declarees par `dagster-postgres`.
5. J'ai inspecte le code du package pour voir quel driver et quel dialecte
   SQLAlchemy Dagster utilisait reellement.
6. J'ai constate que cette version de Dagster utilise `psycopg2`, et non
   `psycopg3`.
7. J'ai aussi constate que SQLAlchemy 2.1 choisissait automatiquement le
   dialecte `psycopg`, ce qui n'etait pas compatible avec Dagster 1.8.12.

Cette verification etait importante : installer un package qui porte un nom
proche de celui mentionne dans l'erreur ne suffisait pas. Il fallait verifier
la compatibilite entre les versions de Dagster, du driver PostgreSQL et de
SQLAlchemy.

### Solution appliquee

J'ai remplace le driver par la version attendue par Dagster et verrouille
SQLAlchemy sur une version compatible :

```text
psycopg2-binary==2.9.9
SQLAlchemy==2.0.36
```

Ensuite, j'ai reconstruit les images avec Docker Compose et verifie que :

- PostgreSQL et RustFS etaient en bonne sante ;
- les buckets `bronze`, `silver` et `gold` etaient crees ;
- le webserver Dagster restait actif ;
- le daemon Dagster restait actif ;
- l'interface Dagster repondait avec le code HTTP `200`.

### Ce que je retiens

Une erreur de dependance doit etre analysee comme un probleme de compatibilite
entre plusieurs packages, et pas seulement comme un package absent.

Pour eviter ce type de probleme, il est utile de :

- verrouiller les versions importantes ;
- lire les logs complets des services ;
- verifier les dependances reelles d'un package ;
- reconstruire et tester les conteneurs apres chaque modification.

## Prochains problemes a documenter

- Migration de MinIO vers RustFS.
- Configuration de Spark avec le stockage S3-compatible.
- Initialisation des buckets au demarrage.
- Gestion des noms et des chemins des fichiers de donnees.
