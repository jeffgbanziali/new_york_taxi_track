from flask import Flask, jsonify, request
import jwt
import datetime
import controllers

app = Flask(__name__)

# Clé secrète pour signer les jetons JWT

SECRET_KEY = "efrei_super_secret_key_2026"

def check_jwt_token():
    """Vérifie et décode le jeton JWT fourni dans l'en-tête Authorization."""
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return False
        
    try:
        token = auth_header.split(" ")[1]
        jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return True
    except Exception:
        return False

# ── Route d'Authentification (Génération du JWT) ────

@app.route('/api/login', methods=['POST'])
def login():
    auth_data = request.json
    if not auth_data or 'username' not in auth_data or 'password' not in auth_data:
        return jsonify({"error": "Missing credentials"}), 400
        
    # Vérification des identifiants (fictifs mais sécurisés pour le projet)
    if auth_data['username'] == 'admin_taxi' and auth_data['password'] == 'Efrei2026!':
        # Génération d'un token valide pour 30 minutes
        token = jwt.encode({
            'user': auth_data['username'],
            'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=30)
        }, SECRET_KEY, algorithm="HS256")
        
        return jsonify({"token": token}), 200
        
    return jsonify({"error": "Invalid username or password"}), 401

# ── Routes ──

@app.route('/api/status', methods=['GET'])
def get_status():
    return jsonify({"status": "online", "layer": "Gold", "security": "JWT"}), 200

@app.route('/api/zones', methods=['GET'])
def api_zones():
    if not check_jwt_token():
        return jsonify({"error": "Unauthorized. Invalid or expired JWT Token"}), 401
        
    borough = request.args.get('borough', None)
    limit = request.args.get('limit', type=int)
    data = controllers.get_zones_data(borough, limit)
    return jsonify(data)

@app.route('/api/heures', methods=['GET'])
def api_heures():
    if not check_jwt_token():
        return jsonify({"error": "Unauthorized"}), 401
    data = controllers.get_heures_data()
    return jsonify(data)

@app.route('/api/paiements', methods=['GET'])
def api_paiements():
    if not check_jwt_token():
        return jsonify({"error": "Unauthorized"}), 401
    borough = request.args.get('borough', None)
    data = controllers.get_paiements_data(borough)
    return jsonify(data)

@app.route('/api/aeroports', methods=['GET'])
def api_aeroports():
    if not check_jwt_token():
        return jsonify({"error": "Unauthorized"}), 401
    data = controllers.get_aeroports_data()
    return jsonify(data)

@app.route('/api/meteo', methods=['GET'])
def api_meteo():
    if not check_jwt_token():
        return jsonify({"error": "Unauthorized"}), 401
    data = controllers.get_meteo_data()
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)