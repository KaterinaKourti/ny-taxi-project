import io
from minio import Minio
import pandas as pd

csv_file_path = "data/test.csv"

# def connect_to_client():
#     client = Minio('127.0.0.1:9000',access_key = 'minioadmin', secret_key = 'minioadmin',secure=False)
#     return client

def create_bucket(bucket):
    client = Minio('127.0.0.1:9000',access_key = 'minioadmin', secret_key = 'minioadmin',secure=False)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    buckets = client.list_buckets()
    for b in buckets:
        print(b.name, b.creation_date)

    return client
#------------------------------------------------------- Preprocess ---------------------------------------------------------------

def preprocess(client,chunk, index):
    
    json = chunk.to_json(orient='records')
    # print(json)
    # Upload data with content-type.
    result = client.put_object("testbucket", f"batch_{index}", io.BytesIO(bytes(json,'ascii')), len(bytes(json,'ascii')), content_type="application/json")

def main():
    client = create_bucket('testbucket')
    df = pd.read_csv(csv_file_path, chunksize=100)
    [preprocess(client,data, index) for index,data in enumerate(df)]


if __name__ == "__main__":
    main()