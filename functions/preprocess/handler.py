from json import loads
from minio import Minio
from os import environ
import pandas as pd
import io


BUCKET_NAMES = {"data-entry": "data-entry", "mapper": "mapper-data-{}"}


def connect_to_minio():
    client = Minio(
        environ.get("MINIO_HOSTNAME"),
        access_key=environ.get("MINIO_ACCESS_KEY"),
        secret_key=environ.get("MINIO_SECRET_KEY"),
        secure=False,
    )
    return client


def preprocess(client, chunk, index, query):
    json_batch = chunk.to_json(orient="records")
    # Upload data with content-type as application/json
    client.put_object(
        BUCKET_NAMES["mapper"].format(f"q{query}"),
        f"batch_{index}",
        io.BytesIO(bytes(json_batch, "ascii")),
        len(bytes(json_batch, "ascii")),
        content_type="application/json",
    )


def handle(req):
    error = ""
    body = loads(req)
    query = body["query"]
    batch_info = body["batch_info"]
    try:
        batch_name = batch_info.split("/")[1]
        index = batch_name.split("_")[1]
        mapper_bucket = BUCKET_NAMES["mapper"].format(f"q{query}")
        client = connect_to_minio()
        print("Batch Name: ", batch_name)

        batch = (
            client.get_object(BUCKET_NAMES["data-entry"], batch_name)
            .read()
            .decode("utf-8")
        )

        data = io.StringIO(batch)
        df = pd.read_csv(data)
        preprocess(client, df, index, query)
    except Exception as e:
        error = e
        print("Error in preprocess handler")

    payload = (
        {"success": False, "error": error}
        if error
        else {
            "success": True,
            "message": f"Preprocess data for q{query} have been successfully uploaded to Minio in bucket {mapper_bucket}",
        }
    )
    return payload
