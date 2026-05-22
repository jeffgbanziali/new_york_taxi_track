# NYC Taxi Real-Time Demand Prediction Lakehouse

![Docker](https://img.shields.io/badge/Docker-Containerized-blue)
![Python](https://img.shields.io/badge/Python-3.9+-yellow)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Distributed-orange)
![Apache Hadoop](https://img.shields.io/badge/Apache%20Hadoop-HDFS-green)
![Hive](https://img.shields.io/badge/Apache%20Hive-Metastore-purple)
![MySQL](https://img.shields.io/badge/MySQL-Gold%20Layer-blue)

---

# Description

Architecture Data Lakehouse complète basée sur le paradigme **Medallion Architecture (Bronze → Silver → Gold)** permettant :

- l’ingestion massive de données NYC Taxi,
- le traitement distribué via Apache Spark,
- le stockage distribué sur HDFS,
- l’enrichissement avec des données météorologiques externes,
- la création de DataMarts analytiques,
- l’exposition sécurisée des KPI via une API REST JWT,
- ainsi qu’un dashboard décisionnel interactif Streamlit.

Cette plateforme reproduit une architecture Big Data moderne inspirée des environnements Data Engineering industriels.

---

# Table des matières

- [Vue d’ensemble](#vue-densemble)
- [Architecture globale](#architecture-globale)
- [Architecture Medallion](#architecture-medallion)
- [Infrastructure Docker](#infrastructure-docker)
- [Arborescence du projet](#arborescence-du-projet)
- [Démarrage rapide](#demarrage-rapide)
- [Configuration environnementale](#configuration-environnementale)
- [Pipeline Bronze](#pipeline-bronze)
- [Pipeline Silver](#pipeline-silver)
- [Pipeline Gold](#pipeline-gold)
- [Sécurisation de l’API REST](#securisation-de-lapi-rest)
- [Dashboard Streamlit](#dashboard-streamlit)
- [Monitoring et observabilité](#monitoring-et-observabilite)
- [Optimisations techniques](#optimisations-techniques)
- [Troubleshooting](#troubleshooting)
- [Limitations et roadmap](#limitations-et-roadmap)
- [FAQ](#faq)

---

# Vue d’ensemble

## Description fonctionnelle

La plateforme traite et valorise des volumes massifs de données liés aux transports urbains de New York.

Le système est capable de :

- ingérer plusieurs millions de lignes de données taxi,
- gérer un pipeline distribué Spark,
- enrichir les données avec des informations météorologiques,
- produire des agrégations métier,
- sécuriser l’accès aux indicateurs,
- alimenter un dashboard analytique temps réel.

---

## Sources de données

### NYC Taxi TLC

Données officielles des trajets taxis de New York :

- plusieurs millions de trajets,
- format Apache Parquet,
- données structurées.

---

### Open-Meteo API

Données météorologiques horaires :

- température,
- précipitations,
- vitesse du vent,
- humidité,
- conditions météo.

---

### Taxi Zone Lookup

Référentiel géographique officiel :

- zones NYC,
- boroughs,
- correspondances des secteurs.

---

# Cas d’usage métier

## Revenue Management

Analyse de l’impact des conditions météo sur :

- la demande,
- les tarifs,
- les comportements utilisateurs.

---

## Operational Dispatch

Optimisation :

- du positionnement des véhicules,
- de la couverture géographique,
- des créneaux horaires.

---

## Weather Impact Analysis

Mesure de l’influence météo sur :

- le trafic,
- les pics de demande,
- les variations de revenus.

---

## Foundation Machine Learning

Préparation de datasets exploitables pour :

- forecasting,
- prédiction de demande,
- modèles ML,
- modèles Deep Learning.

---

# Architecture globale

```text
                               ┌────────────────────┐
                               │    DATA SOURCES    │
                               └─────────┬──────────┘
                                         │
         ┌───────────────────────────────┼──────────────────────────────┐
         │                               │                              │
         ▼                               ▼                              ▼

  NYC Taxi Parquet               Open-Meteo API                 Taxi Zone CSV

         │
         ▼

┌──────────────────────────────────────────────────────────────────────────┐
│                               BRONZE LAYER                              │
│--------------------------------------------------------------------------│
│ HDFS + Apache Parquet + Snappy Compression                               │
│                                                                          │
│ /data/raw/nyc_taxi                                                       │
│ /data/raw/weather                                                        │
│ /data/raw/zone                                                           │
└──────────────────────────────────────────────────────────────────────────┘

         │
         ▼

┌──────────────────────────────────────────────────────────────────────────┐
│                               SILVER LAYER                              │
│--------------------------------------------------------------------------│
│ Apache Spark + Hive                                                      │
│                                                                          │
│ - nettoyage                                                              │
│ - cast des types                                                         │
│ - enrichissement météo                                                   │
│ - enrichissement géographique                                            │
│ - suppression des doublons                                               │
│ - suppression des valeurs aberrantes                                     │
│ - Broadcast Join                                                         │
│ - Window Functions                                                       │
└──────────────────────────────────────────────────────────────────────────┘

         │
         ▼

┌──────────────────────────────────────────────────────────────────────────┐
│                                GOLD LAYER                               │
│--------------------------------------------------------------------------│
│ MySQL DataMarts                                                          │
│                                                                          │
│ - kpi_par_zone                                                           │
│ - kpi_par_heure                                                          │
│ - kpi_paiement                                                           │
│ - kpi_aeroports                                                          │
│ - kpi_meteo_demande                                                      │
└──────────────────────────────────────────────────────────────────────────┘

         │
         ▼

┌──────────────────────────────────────────────────────────────────────────┐
│                           CONSUMPTION LAYER                             │
│--------------------------------------------------------------------------│
│ Flask API + JWT                                                          │
│ Streamlit Dashboard                                                      │
└──────────────────────────────────────────────────────────────────────────┘
```

---

# Architecture Medallion

## Bronze Layer

### Objectif

Stocker les données brutes sans transformation métier.

### Caractéristiques

- stockage HDFS,
- format Parquet,
- compression Snappy,
- partitionnement temporel.

### Scripts

- `feeder.py`
- `api_collector.py`
- `zone_feeder.py`

---

## Silver Layer

### Objectif

Nettoyer, standardiser et enrichir les données.

### Transformations

- suppression des doublons,
- suppression des outliers,
- cast des colonnes,
- enrichissement météo,
- enrichissement géographique,
- jointures optimisées.

### Technologies

- Spark SQL,
- Hive Metastore,
- Broadcast Join,
- Window Functions.

### Script

```text
processor.py
```

---

## Gold Layer

### Objectif

Créer des DataMarts métier optimisés pour l’analyse.

### Tables générées

| Table | Description |
|---|---|
| kpi_par_zone | rentabilité par secteur |
| kpi_par_heure | analyse horaire |
| kpi_paiement | répartition des paiements |
| kpi_aeroports | indicateurs aéroportuaires |
| kpi_meteo_demande | corrélation météo/demande |

### Script

```text
datamart.py
```

---

# Infrastructure Docker

## Services de la plateforme

| Service | Port | Rôle |
|---|---|---|
| namenode | 9870 | HDFS NameNode |
| datanode | 9864 | HDFS DataNode |
| spark-master | 7077 / 8080 | Spark Cluster Master |
| spark-worker-1 | 8081 | Spark Worker |
| spark-worker-2 | 8082 | Spark Worker |
| hive-metastore | 9083 | Hive Metastore |
| hive-server | 10000 | Hive Server2 |
| mysql-gold | 3306 | Base Gold |
| taxi-api | 5000 | API Flask sécurisée |
| taxi-dashboard | 8501 | Dashboard Streamlit |
| mysql-ui | 8085 | phpMyAdmin |

---

# Arborescence du projet

```text
Big_data_framework/
│
├── .env
├── docker-compose.yml
├── convert_snappy.py
├── startup.sh
├── entrypoint.sh
├── hadoop.env
├── hadoop-hive.env
│
├── api/
│   ├── Dockerfile
│   ├── app.py
│   ├── controllers.py
│   └── database.py
│
├── conf/
│   ├── hive-log4j2.properties
│   └── hive-site.xml
│
├── pipeline/
│   ├── api_collector.py
│   ├── feeder.py
│   ├── processor.py
│   ├── datamart.py
│   ├── zone_feeder.py
│   └── mysql-connector-java-8.0.28.jar
│
├── source/
│   ├── yellow/
│   └── zone/
│
├── streamlit/
│   ├── Dockerfile
│   └── dashboard.py
│
└── submit/
    └── submit.sh
```

---

# Démarrage rapide

# 1. Vérifications préalables

```bash
docker --version
docker compose version
python --version
```

---

# 2. Conversion des fichiers Parquet

Les fichiers NYC TLC utilisent le codec `zstd`.

Conversion vers `snappy` :

```bash
python convert_snappy.py
```

---

# 3. Démarrage de l’infrastructure

```bash
docker compose down -v
docker compose up -d --build
```

Attendre environ 60 secondes.

---

# 4. Connexion au cluster Spark

```bash
docker exec -it spark-master bash
```

---

# 5. Lancement du pipeline complet

```bash
bash /opt/pipeline/submit.sh all
```

Le pipeline exécute automatiquement :

- ingestion météo,
- ingestion géographique,
- ingestion taxi,
- nettoyage Silver,
- enrichissement,
- création des KPI Gold.

---

# Configuration environnementale

## Exemple `.env`

```env
MYSQL_ROOT_PASSWORD=root1234
MYSQL_DATABASE=gold
MYSQL_USER=taxi_user
MYSQL_PASSWORD=taxi1234

HADOOP_NAMENODE_DIR=/hadoop/hdfs/namenode
HADOOP_DATANODE_DIR=/hadoop/hdfs/datanode

SPARK_MASTER=spark://spark-master:7077
SPARK_EXECUTOR_MEMORY=1536m
SPARK_EXECUTOR_CORES=2
SPARK_DRIVER_MEMORY=1536m

FLASK_ENV=production
API_PORT=5000
API_HOST=0.0.0.0
```

---

# Pipeline Bronze

## Rôle

Stockage brut des données sources.

## Fonctionnalités

- lecture Parquet,
- standardisation des colonnes,
- partitionnement,
- compression Snappy,
- ingestion météo.

## Partitionnement HDFS

```text
hdfs://namenode:9000/data/raw/nyc_taxi/year=2026/month=05/day=22/
```

---

# Pipeline Silver

## Nettoyage

- suppression des doublons,
- suppression des trajets invalides,
- suppression des valeurs nulles.

## Optimisations

- Broadcast Join,
- cache Spark,
- partitionnement distribué.

## Technologies utilisées

- PySpark,
- Hive,
- Spark SQL.

---

# Pipeline Gold

## Objectif

Créer des indicateurs métier prêts à être consommés.

## Exemples de KPI

- revenu moyen par zone,
- demande par heure,
- impact météo,
- typologie des paiements,
- activité aéroportuaire.

---

# Sécurisation de l’API REST

## Architecture

Le Dashboard Streamlit ne communique jamais directement avec MySQL.

Toutes les requêtes passent par l’API Flask sécurisée.

---

## Authentification JWT

### Login

```http
POST /api/login
```

Réponse :

```json
{
  "token": "JWT_TOKEN"
}
```

---

## Requêtes sécurisées

```http
GET /api/zones
Authorization: Bearer JWT_TOKEN
```

Le décorateur JWT :

- valide la signature,
- valide l’expiration,
- refuse les accès invalides.

---

# Dashboard Streamlit

## Fonctionnalités

- graphiques interactifs,
- KPI dynamiques,
- visualisations géographiques,
- filtres analytiques.

---

## Optimisation Cache

```python
@st.cache_data(ttl=3600)
def load_data_from_api(endpoint, params=None):
    pass
```

---

# Monitoring et observabilité

## Interfaces disponibles

| Service | URL |
|---|---|
| Hadoop HDFS | http://localhost:9870 |
| Spark Cluster | http://localhost:8080 |
| Spark Jobs | http://localhost:4040 |
| phpMyAdmin | http://localhost:8085 |

---

# Optimisations techniques

## Broadcast Join

Les petites tables :

- météo,
- référentiel zones,

sont dupliquées en mémoire sur chaque worker Spark afin d’éviter les opérations de shuffle coûteuses.

---

## Cache Spark

La table Silver est persistée temporairement en RAM via :

```python
.persist()
```

afin d’accélérer les exports Gold.

---

## Compression Snappy

Avantages :

- lecture rapide,
- réduction du stockage,
- meilleure compatibilité Hadoop.

---

# Troubleshooting

# SafeMode Exception

## Erreur

```text
SafeModeException: Cannot create directory
```

## Solution

```bash
docker exec namenode hdfs dfsadmin -safemode leave
```

---

# Erreur Codec Parquet

## Cause

Les fichiers zstd n’ont pas été convertis.

## Solution

```bash
docker compose down -v
python convert_snappy.py
docker compose up -d --build
```

---

# Limitations et roadmap

| Sujet | État actuel | Limitation | Évolution cible |
|---|---|---|---|
| Orchestration | Bash | Pas de reprise sur panne | Apache Airflow |
| Ingestion | Batch | Rafraîchissement J+1 | Kafka + Spark Streaming |
| Stockage | Parquet | Pas d’updates fins | Delta Lake |
| Logs | stdout | Logs non persistants | Stack ELK |

---

# FAQ

## Pourquoi les données 2024 sont stockées sous 2026 ?

Le partitionnement est basé sur :

- la date d’ingestion,
- et non la date métier.

Cela garantit :

- la traçabilité,
- l’auditabilité,
- la reproductibilité.

---

## Pourquoi utiliser une architecture Medallion ?

Cette architecture permet :

- une meilleure gouvernance,
- un découplage des traitements,
- une meilleure maintenabilité,
- une meilleure scalabilité.

---

## Pourquoi utiliser Hive ?

Hive fournit :

- un catalogue centralisé,
- un accès SQL distribué,
- une meilleure interopérabilité Spark.

---

## Pourquoi utiliser JWT ?

JWT permet :

- une authentification stateless,
- une sécurisation simple des APIs,
- une validation rapide des requêtes.

---

# Évolutions futures

## Data Streaming

Migration vers :

- Kafka,
- Spark Structured Streaming.

---

## Lakehouse moderne

Migration vers :

- Delta Lake,
- Iceberg,
- Hudi.

---

## Orchestration industrielle

Ajout :

- Apache Airflow,
- DAGs,
- scheduling,
- retry automatique.

---

## Monitoring avancé

Ajout :

- ELK Stack,
- Prometheus,
- Grafana.

---

# Auteur

Projet Big Data framework 
Efrei Paris — Évaluation 2026