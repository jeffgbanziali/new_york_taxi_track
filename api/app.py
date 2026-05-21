from flask import Flask, jsonify, request
import mysql.connector
from mysql.connector import pooling
import os
import sys

app = Flask(__name__)

# ── Connexion Pool MySQL ──────────────────────────────────────
try:
    db_pool = pooling.MySQLConnectionPool(
        pool_name="gold_pool",
        pool_size=5,
        host=os.environ.get("MYSQL_HOST", "mysql-gold"),
        user=os.environ.get("MYSQL_USER", "taxi_user"),
        password=os.environ.get("MYSQL_PASSWORD", "taxi1234"),
        database=os.environ.get("MYSQL_DATABASE", "gold")
    )
except mysql.connector.Error as err:
    print(f"Erreur d'initialisation du Pool MySQL: {err}")
    sys.exit(1)

def execute_query(query, params=()):
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(query, params)
        result = cursor.fetchall()
        return result
    except mysql.connector.Error as err:
        print(f"Erreur SQL sur la requête [{query}]: {err}")
        return None
    finally:
        cursor.close()
        conn.close()

# ── Routes API ────────────────────────────────────────────────

@app.route('/api/status', methods=['GET'])
def get_status():
    return jsonify({"status": "online", "layer": "Gold", "database": "MySQL"}), 200

@app.route('/api/zones', methods=['GET'])
def get_kpi_zones():
    borough = request.args.get('borough', None)
    limit = request.args.get('limit', type=int)
    
    query = "SELECT borough, zone, nb_trajets, ca_total, tarif_moyen, pourboire_moyen, distance_moy, duree_moy, rang_borough FROM kpi_par_zone"
    params = []
    
    if borough and borough != "Tous":
        query += " WHERE borough = %s"
        params.append(borough)
        
    query += " ORDER BY ca_total DESC"
    
    if limit:
        query += " LIMIT %s"
        params.append(limit)
        
    data = execute_query(query, tuple(params))
    return jsonify(data)

@app.route('/api/heures', methods=['GET'])
def get_kpi_heures():
    query = "SELECT heure, jour_semaine, mois, nb_trajets, ca_total, tarif_moyen FROM kpi_par_heure ORDER BY heure ASC"
    data = execute_query(query)
    return jsonify(data)

@app.route('/api/paiements', methods=['GET'])
def get_kpi_paiements():
    borough = request.args.get('borough', None)
    query = "SELECT borough, mode_paiement, nb_trajets, tarif_moyen FROM kpi_paiement"
    params = []
    if borough and borough != "Tous":
        query += " WHERE borough = %s"
        params.append(borough)
        
    data = execute_query(query, tuple(params))
    return jsonify(data)

@app.route('/api/aeroports', methods=['GET'])
def get_kpi_aeroports():
    # FIX EFFECTUÉ : Suppression de ca_total qui n'a pas été calculé dans Spark pour cette table
    query = """
        SELECT 
            aeroport, 
            nb_trajets, 
            tarif_moyen, 
            distance_moy, 
            duree_moy 
        FROM kpi_aeroports 
        ORDER BY nb_trajets DESC
    """
    data = execute_query(query)
    return jsonify(data)

@app.route('/api/meteo', methods=['GET'])
def get_kpi_meteo():
    query = """
        SELECT 
            meteo, 
            temp_moy, 
            pluie_moy, 
            nb_trajets, 
            tarif_moyen, 
            pourboire_moyen, 
            duree_moy 
        FROM kpi_meteo 
        ORDER BY nb_trajets DESC
    """
    data = execute_query(query)
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)