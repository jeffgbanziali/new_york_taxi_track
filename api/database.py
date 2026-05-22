import mysql.connector
from mysql.connector import pooling
import os
import sys

# Initialisation sécurisée du pool de connexions (via .env)
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
    print(f"Erreur critique d'initialisation du Pool MySQL: {err}")
    sys.exit(1)

def execute_query(query, params=()):
    """Execute une requête SQL de manière sécurisée en reprenant une connexion du Pool."""
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(query, params)
        return cursor.fetchall()
    except mysql.connector.Error as err:
        print(f"Erreur SQL sur la requête [{query}]: {err}")
        return None
    finally:
        cursor.close()
        conn.close()