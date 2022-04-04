
from json import loads
from minio import Minio
from os import environ
import pandas as pd
import io


BUCKET_NAMES = {
    'data-entry': 'data-entry',
    'mapper': 'mapper-data-{}'
}


def create_bucket(bucket):
    client = Minio(environ.get("MINIO_HOSTNAME"),access_key = environ.get("MINIO_ACCESS_KEY"), secret_key = environ.get("MINIO_SECRET_KEY"),secure=False)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    buckets = client.list_buckets()
    for b in buckets:
        print(b.name, b.creation_date)

    return client

# def connect_to_client():
#     client = Minio('127.0.0.1:9000',access_key = 'minioadmin', secret_key = 'minioadmin',secure=False)
#     return client


def preprocess(client,chunk,index,query):
    json_batch = chunk.to_json(orient='records')
    # Upload data with content-type as application/json
    client.put_object(BUCKET_NAMES['mapper'].format(f"q{query}"), f"batch_{index}", io.BytesIO(bytes(json_batch,'ascii')), len(bytes(json_batch,'ascii')), content_type="application/json")



def handle(req):
    error = ''
    try:
        body = loads(req)
        query = body['query']
        batch_info = body['batch_info']
        batch_name = batch_info.split('/')[1]
        index = batch_name.split('_')[1]
        mapper_bucket = BUCKET_NAMES['mapper'].format(f"q{query}")
        client = create_bucket(mapper_bucket)
        print('Batch Name: ',batch_name)
        batch = client.get_object(BUCKET_NAMES['data-entry'],batch_name).read().decode('utf-8')
        # batch_loaded = loads(batch)
        data = io.StringIO(batch)

        df = pd.read_csv(data)
       
        preprocess(client,df,index,query)

    except Exception as e:
        error = e
        print("Error in preprocess handler")

    payload = { 'success': False, 'error': error } if error else { 'success': True, 'message': f'Preprocess data for q1 have been successfully uploaded to Minio in bucket {mapper_bucket}' }

    return payload


# if __name__ == "__main__":
#     main()