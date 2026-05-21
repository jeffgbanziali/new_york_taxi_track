from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import time

# ─────────────────────────────────────────────────────────────
#  datamart.py — Agrégations Gold → MySQL
#
#  Lit Silver depuis Hive → agrégations + window functions
#  → écrit dans MySQL (couche Gold)
#  Inclut les données météo pour corrélation
#
#  Lancement :
#  spark-submit --master spark://spark-master:7077 \
#    --jars /opt/pipeline/mysql-connector-java-8.0.28.jar \
#    /opt/pipeline/datamart.py
# ─────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("datamart")
    .enableHiveSupport()
    .getOrCreate()
)

# ── Connexion MySQL ───────────────────────────────────────────
mysql_url    = "jdbc:mysql://mysql-gold:3306/gold?useSSL=false&allowPublicKeyRetrieval=true"
mysql_user   = "taxi_user"
mysql_pass   = "taxi1234"
mysql_driver = "com.mysql.cj.jdbc.Driver"

def write_mysql(df, table):
    (
        df.write
        .format("jdbc")
        .option("url",      mysql_url)
        .option("dbtable",  table)
        .option("user",     mysql_user)
        .option("password", mysql_pass)
        .option("driver",   mysql_driver)
        .mode("overwrite")
        .save()
    )
    print("Table MySQL creee : {}".format(table))

# ── Lecture Silver depuis Hive ────────────────────────────────
df = spark.table("default.nyc_taxi_silver")

# ── persist() visible dans Spark UI ──────────────────────────
df.persist()
print("Lignes Silver : {}".format(df.count()))

df.createOrReplaceTempView("silver")

# ── time.sleep pour voir dans Spark UI ───────────────────────
time.sleep(120)

# ── DataMart 1 : KPIs par zone + window function ──────────────
window_zone = Window.partitionBy("PU_Borough").orderBy(F.desc("ca_total"))

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

# ── DataMart 2 : KPIs par heure + window function ─────────────
window_heure = Window.partitionBy("pickup_month").orderBy(F.desc("nb_trajets"))

df_heure = (
    spark.sql("""
        SELECT
            pickup_hour                     AS heure,
            CASE pickup_weekday
                WHEN 1 THEN 'Dimanche'
                WHEN 2 THEN 'Lundi'
                WHEN 3 THEN 'Mardi'
                WHEN 4 THEN 'Mercredi'
                WHEN 5 THEN 'Jeudi'
                WHEN 6 THEN 'Vendredi'
                WHEN 7 THEN 'Samedi'
            END                             AS jour_semaine,
            pickup_month                    AS mois,
            COUNT(*)                        AS nb_trajets,
            ROUND(SUM(total_amount), 2)     AS ca_total,
            ROUND(AVG(total_amount), 2)     AS tarif_moyen
        FROM silver
        GROUP BY pickup_hour, pickup_weekday, pickup_month
    """)
    .withColumn("rang_mois", F.rank().over(window_heure))
)
write_mysql(df_heure, "kpi_par_heure")

# ── DataMart 3 : Paiements + window function ──────────────────
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

# ── DataMart 4 : Aeroports ────────────────────────────────────
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

# ── DataMart 5 : Météo + trajets (corrélation) ────────────────
window_meteo = Window.partitionBy("weather_desc").orderBy(F.desc("nb_trajets"))

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
        ORDER BY nb_trajets DESC
    """)
    .withColumn("rang_meteo", F.rank().over(window_meteo))
)
write_mysql(df_meteo, "kpi_meteo")

print("5 DataMarts Gold ecrits dans MySQL !")
spark.stop()