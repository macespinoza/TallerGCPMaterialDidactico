#pip install mysql-connector-python  
#https://us-central1-projeto.cloudfunctions.net/bf1?prj=automatic-tract-396023
import pandas as pd
from flask import Flask, request
import mysql.connector as connection
from google.cloud import bigquery
import mysql
import logging

app = Flask(__name__)

@app.route("/", methods=["GET"])


def entrada():
    logger = logging.getLogger()
    
    request_arg = request.args
    prj = request.args.get("prj","nombreproyecto")

    mydb = mysql.connector.connect(
      host="xx.xx.xxx.xx",
      user="xxx",
      passwd="xxxxx",
      database="xxxxx"
    )
    query = "SELECT * FROM classicmodels.payments;"
    result_dataFrame = pd.read_sql(query,mydb)
    mydb.close() #close the connection 

    client = bigquery.Client()

    table_id =prj+".pe_ci_dtlk_raw.mysql_payments_raw"
    table = client.get_table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=table.schema,
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
    result_dataFrame, table_id, job_config=job_config)  # Make an API request.
    return "Listo"