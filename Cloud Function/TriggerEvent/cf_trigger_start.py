import requests

def cf_trigger_start(request):
    
    
    #Project
    prj = 'silicon-airlock-370404'
    #Dataset
    dataset = 'pe_ci_dtlk_raw'
    #Origen
    origen = 'mysql2'

################TABLA PAYMENTS
    #Table
    tablaoriginal = 'payments'
    #Query de ingesta
    query_origen ='''SELECT * FROM classicmodels.payments;''' 
    
    # definimos los parametros de entrada al endpoint
    PARAMS = {'prj':prj,
              'dataset':dataset,
              'table':tablaoriginal,
              'origen':origen,
              'query_origen':query_origen
             }
    # api-endpoint 
    URL = "https://us-central1-silicon-airlock-370404.cloudfunctions.net/df_mysql_to_bq"
    # enviamos get request y grabamos la respuesta
    r = requests.get(url = URL, params = PARAMS)
    return "listo"