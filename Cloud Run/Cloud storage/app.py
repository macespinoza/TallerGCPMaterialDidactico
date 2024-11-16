import numpy as np
import pandas as pd
from io import BytesIO
from flask import Flask, request
from google.cloud import storage
import logging

app = Flask(__name__)

@app.route("/", methods=["GET"])


def entrada():
    entrada = "output.csv"

    logger = logging.getLogger()

    #""" Return a friendly HTTP greeting. """  
    object_name = request.args.get("nombrefile","imagen.jpg")
    bucket_name_origen = request.args.get("bucketorigen","bucket-data-origen")
    bucket_name_destino = request.args.get("bucketdestino","bucket-data-destino")

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name_origen)
    source_blob = source_bucket.blob(object_name)
    destination_bucket = storage_client.bucket(bucket_name_destino)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, object_name
    )

    return "ok"