import mysql.connector as connection
import pandas as pd
from google.cloud import bigquery
import mysql

def mysqltobq(request):

    request_arg = request.args
    prj = request_arg['prj']

    mydb = mysql.connector.connect(
      host="35.235.114.148",
      user="root",
      passwd="123qaz456",
      database="classicmodels"
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
