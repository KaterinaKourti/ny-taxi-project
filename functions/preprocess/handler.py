# def handle(req):
#     """handle a request to the function
#     Args:
#         req (str): request body
#     """

#     return req
from json import loads
import pandas as pd
from minio import Minio

BUCKET_NAMES = {
    'preprocess': 'batches',
    'mapper': 'mapper-data-{}',
    'reducer': 'reducer-data-{}'
}

DATASET_FILEPATH = 'data/fares.csv'

NY_BASE_LAT = 40.730610
NY_BASE_LONG = -73.935242


def create_bucket(bucket):
    client = Minio('127.0.0.1:9000',access_key = 'minioadmin', secret_key = 'minioadmin',secure=False)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    buckets = client.list_buckets()
    for b in buckets:
        print(b.name, b.creation_date)

    return client

# def connect_to_client():
#     client = Minio('127.0.0.1:9000',access_key = 'minioadmin', secret_key = 'minioadmin',secure=False)
#     return client


def preprocess(client,chunk, index):
    json_batch = chunk.to_json(orient='records')
    # Upload data with content-type as application/json
    client.put_object(BUCKET_NAMES['preprocess'], f"batch_{index}", io.BytesIO(bytes(json_batch,'ascii')), len(bytes(json_batch,'ascii')), content_type="application/json")

def handle(req):
    print(req)
    # preprocess_url = 'http://127.0.0.1:8080/function/preprocess'
    client = create_bucket(BUCKET_NAMES['preprocess'])
    dfs = pd.read_csv(DATASET_FILEPATH, chunksize=100)

    [preprocess(client,data, index) for index,data in enumerate(dfs)]


# if __name__ == "__main__":
#     main()