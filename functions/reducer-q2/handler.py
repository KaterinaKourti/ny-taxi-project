from functools import reduce
import io
from json import loads, dumps
from minio import Minio
from os import environ

BUCKET_NAMES = {"reducer-q2": "reducer-data-q2", "final-q2": "final-results-q2"}

def connect_to_minio():
    client = Minio(
        environ.get("MINIO_HOSTNAME"),
        access_key=environ.get("MINIO_ACCESS_KEY"),
        secret_key=environ.get("MINIO_SECRET_KEY"),
        secure=False,
    )
    return client

def get_effective_trips(batch):
    payload = {"effective_trip_ids": batch["effective"]}
    return payload

def reducer(client, bucket, index, batch):
    reduced_data = get_effective_trips(batch)
    data = dumps(reduced_data)
    client.put_object(
        bucket,
        f"Q2_reducer_results_batch_{index}",
        io.BytesIO(bytes(data, "ascii")),
        len(bytes(data, "ascii")),
        content_type="application/json",
    )


def handle(req):
    error = ""
    try:
        body = loads(req)
        batch_name = body["Key"].split("/")[1]
        index = batch_name.split("_")[-1]
        reducer_bucket = BUCKET_NAMES["reducer-q2"]
        final_bucket = BUCKET_NAMES["final-q2"]
        client = connect_to_minio()
        print("Batch Name: ", batch_name)
        # we must decode the byte array into str and then load it as json
        batch = client.get_object(reducer_bucket, batch_name).read().decode("utf-8")
        json_batch = loads(batch)
        reducer(client, final_bucket, index, json_batch)
    except Exception as e:
        error = e
        print("Error in reducer-q2 handler")
    payload = (
        {"success": False, "error": error}
        if error
        else {
            "success": True,
            "message": f"Reducer data for q2 have been successfully uploaded to Minio in bucket {reducer_bucket}",
        }
    )
    return payload
