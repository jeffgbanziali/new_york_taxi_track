from flask import Flask, jsonify
from flask_cors import CORS
import mysql.connector
import os
import time

app = Flask(__name__)
CORS(app)

def get_db():
    return mysql.connector.connect(
        host=os.environ.get("MYSQL_HOST",     "mysql-gold"),
        user=os.environ.get("MYSQL_USER",     "taxi_user"),
        password=os.environ.get("MYSQL_PASSWORD", "taxi1234"),
        database=os.environ.get("MYSQL_DATABASE", "gold")
    )

# ── Attendre que MySQL soit prêt ──────────────────────────────
def wait_for_mysql(retries=10):
    for i in range(retries):
        try:
            get_db()
            print("MySQL connecté !")
            return
        except Exception:
            print("MySQL pas prêt ({}/{}), attente 5s...".format(i+1, retries))
            time.sleep(5)

wait_for_mysql()

# ── Routes ────────────────────────────────────────────────────

@app.route("/")
def index():
    return jsonify({
        "status": "API NYC Taxi opérationnelle",
        "routes": ["/zones", "/heures", "/paiements", "/aeroports"]
    })

@app.route("/zones")
def zones():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM kpi_par_zone ORDER BY ca_total DESC LIMIT 20")
    return jsonify(cursor.fetchall())

@app.route("/heures")
def heures():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM kpi_par_heure ORDER BY nb_trajets DESC LIMIT 50")
    return jsonify(cursor.fetchall())

@app.route("/paiements")
def paiements():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM kpi_paiement ORDER BY borough, nb_trajets DESC")
    return jsonify(cursor.fetchall())

@app.route("/aeroports")
def aeroports():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM kpi_aeroports ORDER BY nb_trajets DESC")
    return jsonify(cursor.fetchall())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)