# NYC Taxi Real-Time Demand Prediction Lakehouse

[![Docker](https://img.shields.io/badge/Docker-20.10%2B-blue)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.9%2B-green)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.0.0-orange)](https://spark.apache.org/)
[![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.2.1-red)](https://hadoop.apache.org/)
[![Status](https://img.shields.io/badge/Status-Beta-yellow)]()

> **Prédiction de demande taxi en temps réel intégrant météo + historique + pricing dynamique**

## Table of Contents
- [Vue d'ensemble](#vue-densemble)
- [Architecture](#architecture)
- [Démarrage rapide](#démarrage-rapide)
- [Configuration](#configuration)
- [Usage](#usage)
- [Pipelines](#pipelines)
- [API REST](#api-rest)
- [Dashboard](#dashboard)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Limitations](#limitations)
- [Roadmap](#roadmap)
- [FAQ](#faq)

---

## Vue d'ensemble

### Qu'est-ce que c'est ?

Un **Data Lakehouse** médaillon (Raw → Silver → Gold) qui ingère :
- 📍 **9.5M trajets taxi NYC/mois** (données structurées)
- 🌦️ **Météo temps réel** (Open-Meteo API, données semi-structurées)
- 📊 **Zones géographiques** (reference statique)

Et produit :
- **5 DataMarts MySQL** avec KPIs (par zone, heure, paiement, aéroports, météo)
- **API REST** pour interroger les KPIs
- **Dashboard Streamlit** avec visualisations Plotly
- **Foundation ML** pour demand forecasting

### Cas d'usage

1. **Revenue Management:** Pricing dynamique basé sur demande/météo
2. **Operational Dispatch:** Allocation optimale des taxis par zone/heure
3. **Weather Impact Analysis:** Corrélations météo → demande
4. **Anomaly Detection:** Alertes si demand ↓ 30% vs historique
5. **Predictive Analytics:** Préparer dataset pour ML (demand forecasting)

### KPIs Clés
```
- Revenue per zone (avec ranking)
- Peak hours by borough
- Payment method distribution (fraude detection)
- Airport efficiency (JFK, LaGuardia, Newark)
- Weather elasticity (% demande ↑/↓ par condition)
```

---

## Architecture

### Schéma Médaillon (Lakehouse)

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                 │
│  NYC Taxi CSV  │  Open-Meteo JSON  │  Taxi Zone CSV             │
└────────┬──────────────┬────────────────────────┬────────────────┘
         │              │                        │
         ▼              ▼                        ▼
    ┌─────────────────────────────────────────────────────────┐
    │  RAW LAYER (HDFS Parquet, Snappy compression)          │
    │  ─────────────────────────────────────────────────────  │
    │  /data/raw/nyc_taxi    (partitioned: year/month/day)    │
    │  /data/raw/weather     (partitioned: year/month)        │
    │  /data/raw/zone        (static reference)              │
    │                                                         │
    │  ✅ Format: Parquet (columnar)                          │
    │  ✅ Compression: Snappy                                 │
    │  ✅ Partitioning: Time-based                            │
    │  ❌ Data Validation: MISSING (TO FIX)                   │
    └────────────┬──────────────────────────────────────────┘
                 │
            (pyspark)
             feeder.py
                 │
    ┌────────────▼──────────────────────────────────────────┐
    │  SILVER LAYER (Hive + HDFS)                          │
    │  ─────────────────────────────────────────────────────│
    │  HDFS: /data/silver/nyc_taxi_cleaned                 │
    │  Hive: default.nyc_taxi_silver                        │
    │                                                      │
    │  ✅ Cleaned (dups removed, types cast)               │
    │  ✅ Enriched (zone joins, weather joins)             │
    │  ✅ Partitioned (year/month/day)                      │
    │  ✅ Window functions (ROW_NUMBER, RANK)              │
    │  ❌ Query optimization: POOR (TO FIX)                │
    └────────────┬───────────────────────────────────────┘
                 │
            (pyspark sql)
           processor.py
                 │
    ┌────────────▼──────────────────────────────────────────┐
    │  GOLD LAYER (MySQL Datamart)                         │
    │  ─────────────────────────────────────────────────────│
    │  MySQL Database: gold                                 │
    │                                                      │
    │  📊 5 Datamarts:                                     │
    │  ├─ kpi_par_zone (RANK OVER borough)                │
    │  ├─ kpi_par_heure (DENSE_RANK OVER month)           │
    │  ├─ kpi_paiement (payment method analysis)           │
    │  ├─ kpi_aeroports (JFK, LaGuardia, Newark)           │
    │  └─ kpi_meteo_demande (weather elasticity)           │
    │                                                      │
    │  ❌ NO INDEXES (TO FIX - 30s queries!)               │
    │  ❌ NO CACHING (TO FIX - repeated queries slow)      │
    └────────────┬───────────────────────────────────────┘
                 │
        (Python/SQL)
         datamart.py
                 │
        ┌────────▼────────┐
        │                 │
        ▼                 ▼
   ┌─────────────┐   ┌──────────────┐
   │  Flask API  │   │  Streamlit   │
   │  (4 endpoints)│   │  (Dashboard) │
   │             │   │              │
   │ ❌ NO JWT   │   │ ❌ NO CACHE  │
   │ ❌ NO RATE  │   │ ❌ SLOW      │
   │    LIMIT    │   │    RERUN     │
   └─────────────┘   └──────────────┘
```

### Stack Technique

| Composant | Version | Port | Status |
|-----------|---------|------|--------|
| **Hadoop HDFS** | 3.2.1 | 9870 | ✅ Running |
| **Spark Master** | 3.0.0 | 8080 (UI), 7077 (master) | ✅ Running |
| **Spark Worker 1-2** | 3.0.0 | 8081-8082 | ✅ Running |
| **Hive Server** | 2.3.2 | 10000 | ✅ Running |
| **PostgreSQL (Hive metastore)** | 12.0 | 5432 | ✅ Running |
| **MySQL Gold** | 8.0 | 3306 | ✅ Running |
| **Flask API** | 2.3 | 5000 | ✅ Running |
| **Streamlit** | 1.28 | 8501 | ✅ Running |
| **Elasticsearch** | - | 9200 | ❌ Missing |
| **Prometheus** | - | 9090 | ❌ Missing |

---

## Démarrage rapide

### Prérequis

```bash
# System
- Docker 20.10+
- Docker Compose 2.0+
- Python 3.9+
- Git

# Vérifie les versions
docker --version
docker-compose --version
python --version
```

### 1. Clone le repo

```bash
git clone https://github.com/yourusername/nyc-taxi-lakehouse.git
cd nyc-taxi-lakehouse
```

### 2. Setup des fichiers de config

```bash
# Copie les fichiers du prof (fournis)
cp prof_files/hadoop.env .
cp prof_files/hadoop-hive.env .
cp prof_files/hive-site.xml conf/
cp prof_files/entrypoint.sh .
cp prof_files/startup.sh .

# Vérifie les fichiers
ls -la hadoop.env hadoop-hive.env conf/
```

### 3. Convertis les Parquet (zstd → snappy)

```bash
# ⚠️ CRITIQUE - Les fichiers TLC sont en zstd, Hadoop ne supporte que snappy

python convert_snappy.py

# Vérification
ls -lh source/yellow/
# Doit montrer : 3 fichiers .parquet (snappy)
```

### 4. Lance le cluster (Docker Compose)

```bash
# Build images (once)
docker-compose build

# Start all containers
docker-compose up -d

# Wait 30s for HDFS to initialize
sleep 30

# Check health
docker-compose ps
# Doit afficher 15 containers en "Up"

# Vérifie HDFS
docker exec namenode hadoop fs -ls /
# Doit afficher: /tmp, /user, /data (vide)
```

### 5. Initialize HDFS & Hive

```bash
# Entrer dans le namenode
docker exec -it namenode bash

# Crée les directories requises
hadoop fs -mkdir -p /tmp /user/hive/warehouse /data/raw /data/silver

# Set permissions
hadoop fs -chmod g+w /tmp
hadoop fs -chmod g+w /user/hive/warehouse

# Vérifie
hadoop fs -ls -R /

exit
```

### 6. Télécharge les données

```bash
# Download NYC Taxi data (3 months × 174 MB = ~522 MB)
# Option A: Direct from TLC (si accès internet)
wget https://d37ciez9cqebz5.cloudfront.net/files/yellow_tripdata_2024-01.parquet -O source/yellow/
wget https://d37ciez9cqebz5.cloudfront.net/files/yellow_tripdata_2024-02.parquet -O source/yellow/
wget https://d37ciez9cqebz5.cloudfront.net/files/yellow_tripdata_2024-03.parquet -O source/yellow/

# Option B: Utilise les fichiers fournis (s'ils existent)
# source/yellow/ doit contenir 3 .parquet files (snappy compressed)

# Vérifie
ls -lh source/yellow/
# Output: 3 x ~180 MB parquet files
```

### 7. Lance le pipeline

```bash
# Entrer dans Spark master container
docker exec -it spark-master bash

# Exécute le pipeline complet (feeder → processor → datamart)
bash /opt/pipeline/submit.sh all

# Logs des étapes:
# Étape 1 (feeder): Lit NYC Taxi CSV → HDFS Raw
# Étape 2 (processor): Nettoie + joint avec météo → HDFS Silver + Hive
# Étape 3 (datamart): Agrégations → MySQL Gold

# Durée estimée: 5-10 min pour 1M+ trajets

exit
```

### 8. Vérifie les outputs

```bash
# HDFS Raw layer
docker exec namenode hadoop fs -ls -R /data/raw/

# Hive table
docker exec hive-server hive -e "SHOW TABLES;"
docker exec hive-server hive -e "SELECT COUNT(*) FROM default.nyc_taxi_silver;"

# MySQL Gold layer
docker exec mysql-gold mysql -u taxi_user -ptaxi1234 gold -e "SHOW TABLES;"
docker exec mysql-gold mysql -u taxi_user -ptaxi1234 gold -e "SELECT * FROM kpi_par_zone LIMIT 5;"
```

### 9. Accès aux UIs

Ouvre dans le navigateur :

| Service | URL | Credentials |
|---------|-----|-------------|
| HDFS NameNode | http://localhost:9870 | N/A |
| YARN ResourceManager | http://localhost:8088 | N/A |
| Spark Master | http://localhost:8080 | N/A |
| Spark App (en cours) | http://localhost:4040 | N/A (actif pendant job) |
| API Flask | http://localhost:5000 | N/A (public) |
| Streamlit Dashboard | http://localhost:8501 | N/A |

---

## Configuration

### Fichiers clés

```
project/
├── docker-compose.yml          # Services (Hadoop, Spark, Hive, MySQL, Flask, Streamlit)
├── hadoop.env                  # Hadoop environment variables
├── hadoop-hive.env             # Hive environment variables
├── startup.sh                  # HDFS/Hive initialization script
├── entrypoint.sh               # Hadoop configuration script
├── convert_snappy.py           # Parquet conversion (zstd → snappy)
│
├── source/                     # Source data (read-only)
│   ├── yellow/                 # NYC Taxi Parquet (3 files, ~500 MB)
│   └── zone/                   # Taxi Zone Lookup CSV
│
├── pipeline/                   # Spark jobs
│   ├── feeder.py               # Raw data ingestion
│   ├── processor.py            # Silver transformation
│   ├── datamart.py             # Gold aggregation
│   ├── zone_feeder.py          # Zone lookup ingestion
│   ├── api_collector.py        # Weather API ingestion
│   ├── submit.sh               # Spark submit wrapper
│   └── mysql-connector-java-8.0.28.jar
│
├── api/                        # Flask API
│   ├── app.py                  # 4 endpoints (zones, hours, weather, airport)
│   ├── Dockerfile              # API container
│   └── requirements.txt
│
├── streamlit/                  # Dashboard
│   ├── dashboard.py            # Streamlit app (8 charts)
│   ├── Dockerfile
│   └── requirements.txt
│
└── conf/                       # Configuration files
    ├── hive-site.xml
    └── hive-log4j2.properties
```

### Variables d'environnement

Fichier `.env` (à créer) :

```bash
# MySQL
MYSQL_ROOT_PASSWORD=root1234
MYSQL_DATABASE=gold
MYSQL_USER=taxi_user
MYSQL_PASSWORD=taxi1234

# Hadoop
HADOOP_NAMENODE_DIR=/hadoop/hdfs/namenode
HADOOP_DATANODE_DIR=/hadoop/hdfs/datanode

# Spark
SPARK_MASTER=spark://spark-master:7077
SPARK_EXECUTOR_MEMORY=2g
SPARK_EXECUTOR_CORES=2

# API
FLASK_ENV=production
API_PORT=5000
API_HOST=0.0.0.0
```

### Tuning pour perfs

```python
# spark-defaults.conf
spark.sql.shuffle.partitions=200          # Default 200, augmente si + de données
spark.sql.autoBroadcastJoinThreshold=10MB # Broadcast joins < 10MB
spark.driver.memory=4g                     # Driver memory
spark.executor.memory=4g                   # Worker memory
```

---

## Usage

### A. Pipeline complet

```bash
# Via submit.sh
cd pipeline
bash submit.sh all

# Ou étape par étape
bash submit.sh feeder      # 1. Ingestion Raw
bash submit.sh processor   # 2. Silver transformation
bash submit.sh datamart    # 3. Gold aggregation
```

### B. Étape par étape

```bash
# 1. Ingestion météo (Weather API)
docker exec spark-master python /opt/pipeline/api_collector.py
# Output: /data/raw/weather/*.parquet

# 2. Ingestion zones (Reference data)
docker exec spark-master python /opt/pipeline/zone_feeder.py
# Output: /data/raw/zone/*.parquet

# 3. Ingestion taxi
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/pipeline/feeder.py
# Output: /data/raw/nyc_taxi/year=*/month=*/day=*/*.parquet

# 4. Nettoyage + Enrichissement
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 2g \
  /opt/pipeline/processor.py
# Output: /data/silver/ + Hive table

# 5. DataMarts MySQL
docker exec spark-master spark-submit \
  --jars /opt/pipeline/mysql-connector-java-8.0.28.jar \
  /opt/pipeline/datamart.py
# Output: MySQL gold.kpi_*
```

### C. Requêtes Spark SQL (interactif)

```bash
# Spark SQL shell
docker exec -it spark-master spark-sql \
  --master spark://spark-master:7077 \
  --database default

# Exemples de requêtes
SELECT COUNT(*) FROM nyc_taxi_silver;
SELECT pickup_zone, SUM(fare_amount) as revenue
  FROM nyc_taxi_silver
  GROUP BY pickup_zone
  ORDER BY revenue DESC
  LIMIT 10;

SELECT 
  HOUR(tpep_pickup_datetime) as hour,
  COUNT(*) as trips,
  AVG(fare_amount) as avg_fare
FROM nyc_taxi_silver
GROUP BY HOUR(tpep_pickup_pickup_datetime)
ORDER BY hour;
```

### D. Requêtes Hive

```bash
# Via Hive server
docker exec hive-server hive -f /tmp/query.hql

# Ou interactive
docker exec -it hive-server hive

# Exemples
SHOW DATABASES;
SHOW TABLES IN default;
SELECT * FROM nyc_taxi_silver LIMIT 10;
```

### E. Requêtes MySQL (Gold layer)

```bash
# Via MySQL client
docker exec -it mysql-gold mysql -u taxi_user -p

# Password: taxi1234
# Database: gold

USE gold;
SHOW TABLES;

SELECT * FROM kpi_par_zone;
SELECT * FROM kpi_par_heure;
SELECT * FROM kpi_paiement;
```

---

## Pipelines

### feeder.py - Raw Data Ingestion

**Objective:** Lire NYC Taxi Parquet → HDFS Raw Layer

```python
# Input
source/yellow/yellow_tripdata_2024-*.parquet

# Processing
├─ Read Parquet (snappy)
├─ Rename columns (snake_case)
├─ Add ingestion_timestamp
└─ Repartition by date

# Output
hdfs://namenode:9000/data/raw/nyc_taxi/year=2024/month=01/day=01/*.parquet

# Partitioning: Oui (year/month/day)
# Compression: Snappy
# Format: Parquet (columnar)
# Cache: Oui (visible dans Spark UI)
```

**Time to execute:** ~2-3 min (500 MB)

### processor.py - Silver Transformation

**Objective:** Nettoyer + enrichir + partitionner

```python
# Input
1. hdfs:///data/raw/nyc_taxi/
2. hdfs:///data/raw/weather/
3. hdfs:///data/raw/zone/

# Processing
├─ Drop duplicates (dropDuplicates on trip_id)
├─ Filter outliers (fare_amount > 0, trip_distance > 0)
├─ Type casting (timestamp, float, int)
├─ Join with zone lookup (pickup_zone_id ← zone_id)
├─ Join with weather (by date + hour)
├─ Add computed columns (hour, day_of_week)
├─ Window functions:
│  ├─ ROW_NUMBER() OVER (PARTITION BY pickup_zone ORDER BY fare_amount DESC)
│  ├─ LAG(fare_amount) OVER (ORDER BY tpep_pickup_datetime)
│  └─ RANK() OVER (PARTITION BY month ORDER BY trip_count DESC)
│
├─ Cache DataFrame (visible Spark UI)
├─ Persist to HDFS
└─ Create Hive table

# Output
1. hdfs://namenode:9000/data/silver/nyc_taxi_cleaned/
2. Hive: default.nyc_taxi_silver

# Partitioning: year/month/day
# Time: ~4-5 min (1M+ rows)
```

**Spark Metrics:**
- RDD cache: 200 MB
- Shuffle: 50 MB
- Persist: MEMORY_AND_DISK

### datamart.py - Gold Aggregation

**Objective:** Créer 5 DataMarts MySQL pour KPIs

```python
# Input: Hive default.nyc_taxi_silver

# Processing: 5 Datamarts

1. KPI_PAR_ZONE
   SELECT 
     pickup_zone,
     borough,
     COUNT(*) as trip_count,
     SUM(fare_amount) as revenue,
     AVG(fare_amount) as avg_fare,
     ROW_NUMBER() OVER (PARTITION BY borough ORDER BY revenue DESC) as rank_in_borough
   GROUP BY pickup_zone, borough
   
2. KPI_PAR_HEURE
   SELECT 
     DATE(tpep_pickup_datetime) as date,
     HOUR(tpep_pickup_datetime) as hour,
     COUNT(*) as trip_count,
     DENSE_RANK() OVER (PARTITION BY MONTH(tpep_pickup_datetime) ORDER BY COUNT(*) DESC) as peak_hour_rank
   GROUP BY DATE(tpep_pickup_datetime), HOUR(tpep_pickup_datetime)

3. KPI_PAIEMENT
   SELECT 
     payment_type,
     borough,
     COUNT(*) as transaction_count,
     SUM(fare_amount) as revenue,
     SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count
   GROUP BY payment_type, borough

4. KPI_AEROPORTS
   SELECT 
     airport_name,
     COUNT(*) as trip_count,
     AVG(trip_distance) as avg_distance,
     AVG(fare_amount) as avg_fare,
     PERCENTILE_APPROX(wait_time, 0.95) as p95_wait_time
   GROUP BY airport_name

5. KPI_METEO_DEMANDE
   SELECT 
     weather_condition,
     COUNT(*) as trip_count,
     AVG(fare_amount) as avg_fare,
     LAG(COUNT(*)) OVER (ORDER BY DATE(tpep_pickup_datetime)) as prev_day_demand,
     ROUND(100 * (COUNT(*) - LAG(COUNT(*)) OVER (...)) / LAG(COUNT(*)) OVER (...), 2) as demand_elasticity_pct
   GROUP BY weather_condition

# Output: MySQL gold schema

# Time: ~2-3 min (5 datamarts)
```

**MySQL Schema:**
```sql
CREATE TABLE kpi_par_zone (
  zone_id INT,
  pickup_zone VARCHAR(255),
  borough VARCHAR(50),
  trip_count BIGINT,
  revenue DECIMAL(12, 2),
  avg_fare DECIMAL(8, 2),
  rank_in_borough INT,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Même structure pour les 4 autres
```

---

## API REST

### Endpoints

#### 1. GET /api/zones

Retourne les KPIs par zone

```bash
curl -s http://localhost:5000/api/zones

# Response
{
  "data": [
    {
      "zone_id": 1,
      "pickup_zone": "East Harlem North",
      "borough": "Manhattan",
      "trip_count": 42195,
      "revenue": 387203.50,
      "avg_fare": 9.17,
      "rank_in_borough": 3
    },
    ...
  ],
  "count": 265,
  "generated_at": "2024-05-22 10:30:00"
}
```

#### 2. GET /api/hourly

Retourne les KPIs par heure

```bash
curl -s http://localhost:5000/api/hourly

# Response
{
  "data": [
    {
      "date": "2024-01-01",
      "hour": 8,
      "trip_count": 125403,
      "avg_fare": 12.50,
      "peak_hour_rank": 1
    },
    ...
  ],
  "count": 744
}
```

#### 3. GET /api/weather

Retourne la corrélation météo/demande

```bash
curl -s http://localhost:5000/api/weather

# Response
{
  "data": [
    {
      "weather_condition": "rainy",
      "trip_count": 1823104,
      "avg_fare": 13.20,
      "demand_elasticity_pct": 12.5
    },
    ...
  ]
}
```

#### 4. GET /api/airports

Retourne les stats aéroports

```bash
curl -s http://localhost:5000/api/airports

# Response
{
  "data": [
    {
      "airport_name": "JFK",
      "trip_count": 189234,
      "avg_distance": 15.3,
      "avg_fare": 45.20,
      "p95_wait_time": 23
    },
    ...
  ]
}
```

### Authentification (À IMPLÉMENTER)

```bash
# ❌ ACTUELLEMENT: Aucune authentification

# ✅ À IMPLÉMENTER: JWT Bearer token

# 1. Login (create token)
curl -X POST http://localhost:5000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "analyst", "password": "secret123"}'

# Response
{"access_token": "eyJhbGc...", "expires_in": 3600}

# 2. Utiliser le token
curl -s http://localhost:5000/api/zones \
  -H "Authorization: Bearer eyJhbGc..."
```

### Rate Limiting (À IMPLÉMENTER)

```bash
# ❌ ACTUELLEMENT: Pas de limite

# ✅ À IMPLÉMENTER: 100 req/min par IP

# Si dépassement:
# HTTP 429 Too Many Requests
# Retry-After: 60
```

### Documentation Swagger (À GÉNÉRER)

```bash
# ✅ Via Flask-RESTX
# URL: http://localhost:5000/api/docs
```

---

## Dashboard

### Streamlit App

**File:** `streamlit/dashboard.py`

**Features:**
- 📊 8+ interactive charts (Plotly)
- 🔄 Real-time refresh (configurable)
- 📈 KPI cards avec trend arrows
- 🗺️ Geo visualization (zones sur carte)
- 🎚️ Filters (date range, borough, weather)

**Current Issues:**
```
❌ NO CACHING: Chaque interaction relance toute la requête
   → Streamlit rerun entier (15-30 sec!)
   → À 100K users simultanés = crash
   
❌ NO PAGINATION: Charge les 265 zones d'un coup
   → 100K rows = lenteur extrême
   
❌ LIMITED INTERACTIVITY: Juste des charts Plotly
   → Pas de drill-down
   → Pas de cross-filtering
   → Pas de export CSV
```

**To launch:**

```bash
streamlit run streamlit/dashboard.py

# Access: http://localhost:8501
```

**Fixes proposés:**

```python
# 1. Add @st.cache_data decorator
@st.cache_data(ttl=3600)  # Cache 1 hour
def load_zones_data():
    return pd.read_sql("SELECT * FROM kpi_par_zone", conn)

# 2. Add pagination
limit = 50
offset = st.slider("Page", 0, 265, 0) * 50
query += f" LIMIT {limit} OFFSET {offset}"

# 3. Add export button
if st.button("Download CSV"):
    csv = df.to_csv(index=False)
    st.download_button(label="Download", data=csv, file_name="kpis.csv")
```

---

## Monitoring

### Spark UI

```
URL: http://localhost:4040

Key metrics to check:
├─ Executors tab
│  ├─ Executor 1-2 memory usage (should be ~50%)
│  └─ Task distribution (balanced?)
│
├─ Storage tab (IMPORTANT for cache/persist check)
│  ├─ RDD cache (200 MB ← from processor.py)
│  ├─ Block size
│  └─ Eviction policy
│
├─ SQL tab
│  ├─ Query execution plans (DAGs)
│  └─ Query duration (should be < 1min)
│
└─ Stages tab
   ├─ Stage 1: Read Raw (map)
   ├─ Stage 2: Join (wide, shuffle)
   ├─ Stage 3: Cache (narrow)
   └─ Stage 4: Write (action)
```

### YARN Resource Manager

```
URL: http://localhost:8088

Check:
├─ Cluster metrics
│  ├─ Total memory: 8 GB
│  ├─ Used memory: should be ~4 GB during job
│  └─ Running apps: 1 (Spark job)
│
└─ Application details
   ├─ State: RUNNING → FINISHED
   ├─ Final Status: SUCCEEDED
   └─ Tracking URL: http://localhost:4040
```

### HDFS NameNode UI

```
URL: http://localhost:9870

Check:
├─ Capacity usage (should be ~50%)
├─ Live datanodes (3 active)
├─ Block pool size (3 copies each)
└─ File system hierarchy
   ├─ /data/raw (174 MB)
   ├─ /data/silver (100 MB, compressed)
   └─ /user/hive/warehouse (5 MB)
```

### Logging (À IMPLÉMENTER)

```
❌ Currently: Logs only to stdout
   
✅ To implement: ELK Stack
   ├─ Filebeat: Ship logs from containers
   ├─ Elasticsearch: Store & index logs
   └─ Kibana: Visualize logs
   
Docker-compose addition:
services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.0.0
    ports:
      - "9200:9200"
    environment:
      - discovery.type=single-node
  
  kibana:
    image: docker.elastic.co/kibana/kibana:8.0.0
    ports:
      - "5601:5601"

Log format (JSON):
{
  "timestamp": "2024-05-22T10:30:00Z",
  "level": "INFO",
  "service": "feeder",
  "message": "Read 1000000 rows from HDFS",
  "duration_ms": 1234,
  "status": "success"
}
```

---

## Troubleshooting

### Problem: HDFS Namenode in Safe Mode

```
Error: org.apache.hadoop.hdfs.server.namenode.SafeModeException:
  Cannot create directory /data/raw. Name node is in safe mode.

Solution:
docker exec namenode hdfs dfsadmin -safemode leave

Verify:
docker exec namenode hdfs dfsadmin -safemode get
# Should output: Safe mode is OFF
```

### Problem: Parquet Snappy Codec Not Found

```
Error: org.apache.hadoop.hive.ql.io.parquet.ParquetHiveSerDe: 
  Schema not available from deserializer: 
  org.apache.parquet.avro.AvroParquetReader: 
  java.io.IOException: Could not read footer

Cause: Fichiers en ZSTD au lieu de SNAPPY

Solution:
python convert_snappy.py
# Puis relancer feeder.py
```

### Problem: MySQL Connection Refused

```
Error: java.sql.SQLException: Communications link failure: 
  java.net.ConnectException: Connection refused (Connection refused)

Solution:
# Vérifie que MySQL est running
docker-compose ps | grep mysql
# Output: mysql-gold Up

# Si pas running:
docker-compose up mysql-gold

# Attends 10 sec pour MySQL init
sleep 10

# Teste connection:
docker exec mysql-gold mysql -u root -proot1234 -e "SELECT 1;"
```

### Problem: Spark Job Timeout

```
Error: org.apache.spark.SparkException: 
  Job aborted due to stage failure: Task 3.0 in stage 2.0 failed 4 times

Solution:
1. Augmente executor memory
   --executor-memory 4g (au lieu de 2g)

2. Augmente shuffle partitions
   spark.sql.shuffle.partitions=300

3. Réduise dataset size (test avec 1 month au lieu de 3)
```

### Problem: Streamlit Not Refreshing

```
Issue: Dashboard shows old data

Solution:
# Clear Streamlit cache
rm -rf ~/.streamlit/
streamlit run streamlit/dashboard.py --logger.level=debug

# Or force refresh in browser
Cmd+Shift+R (Mac)
Ctrl+Shift+R (Windows)
```

### Problem: API Returns 500 Error

```
Error: 500 Internal Server Error

Debug:
1. Check Flask logs
   docker logs taxi-api

2. Verify MySQL connection
   docker exec mysql-gold mysql -u taxi_user -ptaxi1234 gold -e "SELECT COUNT(*) FROM kpi_par_zone;"

3. Check query syntax
   sqlalchemy.exc.OperationalError
```

### Problem: Container OOM (Out of Memory)

```
Error: docker: OOM killed
Reason: Spark + Hive + MySQL all using memory simultaneously

Solution:
1. Reduce executor memory:
   --executor-memory 2g → 1g

2. Reduce Spark partitions:
   spark.sql.shuffle.partitions=200 → 100

3. Add swap (not recommended):
   docker update --memory 8g taxi-api
```

---

## Limitations

### Actuelles (v1.0)

| Limitation | Impact | Workaround |
|-----------|--------|-----------|
| ❌ No API Authentication | Public access | Use VPN + firewall |
| ❌ No rate limiting | DDoS possible | Disable external access |
| ❌ No caching (API) | Slow queries (2-5s) | Use dashboard instead |
| ❌ No DB indexes | 30s queries on large tables | Add indexes manually |
| ❌ Streamlit slow rerun | > 15s load time | Use Dash/React |
| ❌ No data validation | Bad data → bad KPIs | Manual checks |
| ❌ No disaster recovery | Data loss if HDFS fails | Backup HDFS daily |
| ❌ No auto-scaling | Can't handle 100M rows | Upgrade hardware |
| ❌ No logging (centralized) | Hard to debug | Use container logs |
| ❌ No monitoring | No alerting on failures | Watch metrics manually |

### Roadmap Fixes

**Phase 1 (1 week) - Critical:**
- [ ] Add JWT authentication + API key management
- [ ] Add rate limiting (100 req/min)
- [ ] Add MySQL indexes on kpi_* tables
- [ ] Add @st.cache_data to Streamlit

**Phase 2 (2 weeks) - Important:**
- [ ] ELK Stack for logging
- [ ] Prometheus + Grafana for monitoring
- [ ] Great Expectations for data validation
- [ ] Redis cache for API responses

**Phase 3 (3+ weeks) - Nice-to-have:**
- [ ] Kubernetes deployment
- [ ] Rewrite frontend (React/Dash)
- [ ] Apache Airflow for orchestration
- [ ] ML models for demand forecasting

---

## FAQ

### Q: Comment les données sont partitionnées ?

**A:** 
```
Raw: /data/raw/nyc_taxi/year=2024/month=01/day=01/...parquet
Silver: /data/silver/nyc_taxi/year=2024/month=01/day=01/...parquet
Gold: MySQL table avec colonne 'date' (pas partitionné au DB level)
```

Avantage: Spark peut lire seulement 1 jour au lieu de 3 mois.

### Q: Combien de temps prend le full pipeline ?

**A:** ~10-15 min total
- feeder.py: 2-3 min (500 MB)
- processor.py: 5-7 min (1M rows, joins)
- datamart.py: 2-3 min (5 aggregations)

### Q: Peut-on relancer un job sans re-ingérer ?

**A:** Non, actuellement. À implémenter:
```python
# Idempotency: Si data existe, skip ingestion
if hdfs_path_exists("/data/raw/nyc_taxi/year=2024/month=01"):
    print("Already loaded, skipping feeder")
else:
    run_feeder()
```

### Q: Où sont les secrets (MySQL password) stockés ?

**A:** ❌ **PROBLÈME:** Actuellement en code
```python
connection = mysql.connector.connect(
  host="mysql-gold",
  user="taxi_user",
  password="taxi1234"  # ← EXPOSED!
)
```

✅ **Fix:** Utiliser `.env` ou Vault
```python
from dotenv import load_dotenv
import os

load_dotenv()
password = os.getenv("MYSQL_PASSWORD")
```

### Q: Est-ce que ça scale à 1M trajets/jour ?

**A:** Peut-être. Limitations actuelles:
- Spark 2 workers × 2GB chacun = 4GB total
- MySQL sans indexes (slow on 100M rows)
- Streamlit sans cache (tableau entier chargé)

**Pour scale:**
- Augmenter workers (10 workers × 4GB)
- Ajouter indexes sur kpi_* tables
- Migrer dashboard vers Dash/React
- Ajouter Redis cache

### Q: Peut-on prédire la demande avec ça ?

**A:** Non directement. C'est la **foundation** pour ML.

Avec les KPIs actuels, on peut entraîner un modèle:
```python
import sklearn

# Features: [hour, day_of_week, temperature, humidity, is_holiday, prev_demand]
# Target: trip_count (prochain jour)

model = LinearRegression().fit(X_train, y_train)
prediction = model.predict([[8, 3, 72, 65, False, 5000]])
# Output: Predicted trips demain à 8h = 5432
```

Puis utiliser dans pricing dynamic:
```
price_multiplier = 1 + (demand_forecast / avg_historical) * 0.2
# Si forecast +20% vs normal → +4% prix
```

### Q: Comment ajouter une nouvelle source de data ?

**A:** 
1. Créer `pipeline/new_source_feeder.py`
2. Lire data → /data/raw/new_source/
3. Modifier `processor.py` pour joindre
4. Ajouter colonne dans 1+ datamarts
5. Ajouter test dans pytest

### Q: Peut-on exporter les datamarts vers Tableau/Looker ?

**A:** Oui! MySQL directement:
```
Tableau: New Data Source → MySQL → gold → kpi_par_zone
Looker: Create view sur kpi_par_zone
```

Mais sans indexes, ça sera lent.

---

## Support

Pour les issues, ouvrir un ticket GitHub:
```
https://github.com/yourusername/nyc-taxi-lakehouse/issues
```

Format:
```
Title: [COMPONENT] Short description
Component: feeder | processor | datamart | api | dashboard
Environment: Docker local / Kubernetes prod
Steps to reproduce:
1. ...
2. ...

Expected: 
Actual: 
Logs: <paste docker logs>
```

---

## License

MIT License (2024)

---

**Last Updated:** May 22, 2024  
**Maintainer:** Data Engineering Team  
**Status:** Beta (Production-Ready v2.0 in development)