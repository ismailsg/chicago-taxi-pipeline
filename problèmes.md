# Problèmes rencontrés et résolutions

Ce fichier rassemble les problèmes rencontrés pendant le développement du pipeline,
ainsi que la démarche suivie pour les comprendre et les résoudre.

L'objectif n'est pas seulement de noter la solution finale. Je veux aussi garder
une trace des erreurs observées, des vérifications effectuées et de ce que j'en
retiendrai pour la suite.

## 1. Incompatibilité de versions avec PostgreSQL et Dagster

### Contexte

Le projet utilise Dagster avec PostgreSQL pour stocker les informations de
l'orchestrateur. Les dépendances principales étaient notamment :

- `dagster==1.8.12`
- `dagster-postgres==0.24.12`
- SQLAlchemy

Après le passage à RustFS, les conteneurs RustFS et l'initialisation des buckets
fonctionnaient correctement. Pourtant, les conteneurs Dagster s'arrêtaient.


### Démarche de résolution

J'ai suivi plusieurs étapes :

1. J'ai consulté les logs des conteneurs `dagster-webserver` et
   `dagster-daemon`.
2. J'ai vérifié que PostgreSQL était bien démarré et en bonne santé.
3. J'ai testé la connexion à PostgreSQL depuis une image Python.
4. J'ai regardé les dépendances déclarées par `dagster-postgres`.
5. J'ai inspecté le code du package pour voir quel driver et quel dialecte
   SQLAlchemy Dagster utilisait réellement.
6. J'ai constaté que cette version de Dagster utilise `psycopg2`, et non
   `psycopg3`.
7. J'ai aussi constaté que SQLAlchemy 2.1 choisissait automatiquement le
   dialecte `psycopg`, ce qui n'était pas compatible avec Dagster 1.8.12.

Cette vérification était importante : installer un package qui porte un nom
proche de celui mentionné dans l'erreur ne suffisait pas. Il fallait vérifier
la compatibilité entre les versions de Dagster, du driver PostgreSQL et de
SQLAlchemy.

### Solution appliquée

J'ai remplacé le driver par la version attendue par Dagster et verrouillé
SQLAlchemy sur une version compatible :

```text
psycopg2-binary==2.9.9
SQLAlchemy==2.0.36
