from json import loads,dumps
import pandas as pd
from minio import Minio
from os import getcwd

BUCKET_NAMES = {
    'data_entry': 'data-entry'
}

DATASET_FILEPATH = getcwd()+'/data/test.csv'

def create_bucket(bucket):
    client = Minio('192.168.68.108:9000',access_key = 'minioadmin', secret_key = 'minioadmin',secure=False)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    buckets = client.list_buckets()
    for b in buckets:
        print(b.name, b.creation_date)

    return client

# def connect_to_client():
#     client = Minio('127.0.0.1:9000',access_key = 'minioadmin', secret_key = 'minioadmin',secure=False)
#     return client


def data_entry(client,chunk, index):
    # Upload data with content-type as application/json
    client.put_object(BUCKET_NAMES['data_entry'], f"batch_{index}", io.BytesIO(bytes(chunk,'ascii')), len(bytes(chunk,'ascii')), content_type="application/csv")

def handle(req):
    # data_entry_url = 'http://127.0.0.1:8080/function/data_entry'
    client = create_bucket(BUCKET_NAMES['data_entry'])
    dfs = pd.read_csv(DATASET_FILEPATH)
    client.put_object(BUCKET_NAMES['data_entry'], f"batch_{index}", io.BytesIO(bytes(dfs,'ascii')), len(bytes(dfs,'ascii')), content_type="application/csv")
    

    resp = { 'success': True }
    
    print(dumps(resp))


    # [data_entry(client,data, index) for index,data in enumerate(dfs)]
    
