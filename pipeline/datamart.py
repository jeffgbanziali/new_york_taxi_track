from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import os
import time

# ─────────────────────────────────────────────────────────────
#  datamart.py — Agrégations Gold → MySQL
# ─────────────────────────────────────────────────────────────

# Initialisation de la session connectée au catalogue centralisé Hive
spark = (
    SparkSession.builder
    .appName("datamart")
    .enableHiveSupport()
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .getOrCreate()
)

# ── Connexion MySQL Sécurisée (Lecture via le fichier .env) ──
mysql_host  = os.environ.get("MYSQL_HOST", "mysql-gold")
mysql_db    = os.environ.get("MYSQL_DATABASE", "gold")
mysql_user  = os.environ.get("MYSQL_USER", "taxi_user")
mysql_pass  = os.environ.get("MYSQL_PASSWORD", "taxi1234")

mysql_url    = f"jdbc:mysql://{mysql_host}:3306/{mysql_db}?useSSL=false&allowPublicKeyRetrieval=true"
mysql_driver = "com.mysql.cj.jdbc.Driver"

def write_mysql(df, table):
    (
        # .coalesce(1) évite que Spark n'ouvre des dizaines de 
        # connexions simultanées, ce qui ferait planter ton conteneur MySQL.
        df.coalesce(1)
        .write
        .format("jdbc")
        .option("url",      mysql_url)
        .option("dbtable",  table)
        .option("user",     mysql_user)
        .option("password", mysql_pass)
        .option("driver",   mysql_driver)
        .mode("overwrite")
        .save()
    )
    print("Table MySQL creee avec succes : {}".format(table))

# ── Lecture Silver depuis Hive ────────────────────────────────
print("Lecture de la table nyc_taxi_silver depuis Hive...")
df = spark.table("default.nyc_taxi_silver")

# Persistance en mémoire pour accélérer les 5 calculs suivants
df.persist()
print("Lignes Silver à traiter : {}".format(df.count()))

df.createOrReplaceTempView("silver")

# Pause pour te laisser le temps de capturer l'UI YARN (http://localhost:8088)
print("Pause de 120s pour inspection UI...")
time.sleep(120)

# ── DataMart 1 : KPIs par zone ────────────────────────────────
print("Calcul du DataMart 1 : kpi_par_zone...")
# FIX SYNTAXE : On applique le tri sur le nom calculé dans le SELECT ('ca_total')
window_zone = Window.partitionBy("borough").orderBy(F.desc("ca_total"))

df_zone = (
    spark.sql("""
        SELECT
            PU_Borough                       AS borough,
            PU_Zone                          AS zone,
            COUNT(*)                         AS nb_trajets,
            ROUND(SUM(total_amount), 2)      AS ca_total,
            ROUND(AVG(total_amount), 2)      AS tarif_moyen,
            ROUND(AVG(tip_amount), 2)        AS pourboire_moyen,
            ROUND(AVG(trip_distance), 2)     AS distance_moy,
            ROUND(AVG(trip_duration_min), 1) AS duree_moy
        FROM silver
        WHERE PU_Borough IS NOT NULL
        GROUP BY PU_Borough, PU_Zone
    """)
    .withColumn("rang_borough", F.rank().over(window_zone))
)
write_mysql(df_zone, "kpi_par_zone")

# ── DataMart 2 : KPIs par heure ───────────────────────────────
print("Calcul du DataMart 2 : kpi_par_heure...")
window_heure = Window.partitionBy("mois").orderBy(F.desc("nb_trajets"))

df_heure = (
    spark.sql("""
        SELECT
            pickup_hour                                     AS heure,
            CASE pickup_weekday
                WHEN 1 THEN 'Dimanche'
                WHEN 2 THEN 'Lundi'
                WHEN 3 THEN 'Mardi'
                WHEN 4 THEN 'Mercredi'
                WHEN 5 THEN 'Jeudi'
                WHEN 6 THEN 'Vendredi'
                WHEN 7 THEN 'Samedi'
            END                                             AS jour_semaine,
            pickup_month                                    AS mois,
            COUNT(*)                                        AS nb_trajets,
            ROUND(SUM(total_amount), 2)                     AS ca_total,
            ROUND(AVG(total_amount), 2)                     AS tarif_moyen
        FROM silver
        GROUP BY pickup_hour, pickup_weekday, pickup_month
    """)
    .withColumn("rang_mois", F.rank().over(window_heure))
)
write_mysql(df_heure, "kpi_par_heure")

# ── DataMart 3 : Mode de Paiement ─────────────────────────────
print("Calcul du DataMart 3 : kpi_paiement...")
window_paiement = Window.partitionBy("borough").orderBy(F.desc("nb_trajets"))

df_paiement = (
    spark.sql("""
        SELECT
            PU_Borough AS borough,
            CASE payment_type
                WHEN 1 THEN 'Carte'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'Gratuit'
                WHEN 4 THEN 'Litige'
                ELSE 'Inconnu'
            END        AS mode_paiement,
            COUNT(*)   AS nb_trajets,
            ROUND(AVG(total_amount), 2) AS tarif_moyen
        FROM silver
        WHERE PU_Borough IS NOT NULL
        GROUP BY PU_Borough, payment_type
    """)
    .withColumn("rang_paiement", F.rank().over(window_paiement))
)
write_mysql(df_paiement, "kpi_paiement")

# ── DataMart 4 : Aéroports ────────────────────────────────────
print("Calcul du DataMart 4 : kpi_aeroports...")
df_aeroport = spark.sql("""
    SELECT
        DO_Zone                          AS aeroport,
        COUNT(*)                         AS nb_trajets,
        ROUND(AVG(total_amount), 2)      AS tarif_moyen,
        ROUND(AVG(trip_distance), 2)     AS distance_moy,
        ROUND(AVG(trip_duration_min), 1) AS duree_moy
    FROM silver
    WHERE DO_Zone IN ('JFK Airport', 'LaGuardia Airport', 'Newark Airport')
    GROUP BY DO_Zone
    ORDER BY nb_trajets DESC
""")
write_mysql(df_aeroport, "kpi_aeroports")

# ── DataMart 5 : Météo & Corrélations ─────────────────────────
print("Calcul du DataMart 5 : kpi_meteo...")
window_meteo = Window.partitionBy("meteo").orderBy(F.desc("nb_trajets"))

df_meteo = (
    spark.sql("""
        SELECT
            weather_desc                     AS meteo,
            ROUND(AVG(temperature), 1)       AS temp_moy,
            ROUND(AVG(precipitation), 2)     AS pluie_moy,
            COUNT(*)                         AS nb_trajets,
            ROUND(AVG(total_amount), 2)      AS tarif_moyen,
            ROUND(AVG(tip_amount), 2)        AS pourboire_moyen,
            ROUND(AVG(trip_duration_min), 1) AS duree_moy
        FROM silver
        WHERE weather_desc IS NOT NULL
        GROUP BY weather_desc
    """)
    .withColumn("rang_meteo", F.rank().over(window_meteo))
)
write_mysql(df_meteo, "kpi_meteo")

print("---")
print("Succes : Les 5 DataMarts Gold ont été écrits dans MySQL-Gold !")
print("---")

spark.stop()