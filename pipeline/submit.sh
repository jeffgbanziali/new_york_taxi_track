#!/bin/bash

# ─────────────────────────────────────────────────────────────
#  submit.sh — Lance les 4 pipelines Spark dans l'ordre
#
#  Usage depuis spark-master :
#    bash /opt/pipeline/submit.sh api_collector
#    bash /opt/pipeline/submit.sh feeder
#    bash /opt/pipeline/submit.sh processor
#    bash /opt/pipeline/submit.sh datamart
#    bash /opt/pipeline/submit.sh all
# ─────────────────────────────────────────────────────────────

SPARK_SUBMIT="/spark/bin/spark-submit"
MASTER="spark://spark-master:7077"
PIPELINE_DIR="/opt/pipeline"
MYSQL_JAR="$PIPELINE_DIR/mysql-connector-java-8.0.28.jar"

export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

case "$1" in
  api_collector)
    echo ">>> Lancement api_collector.py (meteo Open-Meteo)..."
    $SPARK_SUBMIT \
      --master $MASTER \
      $PIPELINE_DIR/api_collector.py
    ;;

  feeder)
    echo ">>> Lancement feeder.py (ingestion taxi)..."
    $SPARK_SUBMIT \
      --master $MASTER \
      --executor-memory 512m \
      --driver-memory 512m \
      $PIPELINE_DIR/feeder.py
    ;;

  processor)
    echo ">>> Lancement processor.py (nettoyage + jointure meteo)..."
    $SPARK_SUBMIT \
      --master $MASTER \
      --executor-memory 1536m \
      --driver-memory 1536m \
      $PIPELINE_DIR/processor.py
    ;;

  datamart)
    echo ">>> Lancement datamart.py (agregations Gold -> MySQL)..."
    $SPARK_SUBMIT \
      --master $MASTER \
      --executor-memory 512m \
      --driver-memory 512m \
      --jars $MYSQL_JAR \
      $PIPELINE_DIR/datamart.py
    ;;

  all)
    echo ">>> Lancement pipeline complet..."
    bash $0 api_collector
    bash $0 feeder
    bash $0 processor
    bash $0 datamart
    echo ">>> Pipeline complet termine !"
    ;;

  *)
    echo "Usage : bash submit.sh [api_collector|feeder|processor|datamart|all]"
    exit 1
    ;;
esac