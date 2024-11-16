import pandas as pd
from flask import Flask, request, jsonify
import psycopg2
import logging

app = Flask(__name__)

@app.route("/", methods=["GET"])


def entrada():
    logger = logging.getLogger()
    
    # Parámetros de la base de datos PostgreSQL
    db_config = {
        'dbname': 'data', # Reemplaza con tu nombre de base de datos
        'user': 'postgres',  # Reemplaza con tu nombre de usuario
        'password': '********', # Reemplaza con tu clave
        'host': '99.99.99.99',   # Usar la IP privada para acceder desde GCP
        'port': '5432' #Remplazar con el puerto a usar
    }
    
    try:
        # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect(**db_config)
        query = "SELECT * FROM clientes;" 
        
        # Leer los datos en un DataFrame
        result_dataFrame = pd.read_sql(query, connection)
        
        # Convertir el DataFrame a JSON
        result_json = result_dataFrame.to_json(orient="records")
        
        return jsonify(result_json)
    
    except Exception as error:
        logger.error(f"Error al conectar a la base de datos: {error}")
        return jsonify({"error": str(error)})
    
    finally:
        # Cerrar la conexión
        if connection:
            connection.close()
            logger.info("Conexión cerrada.")