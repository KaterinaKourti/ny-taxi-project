import io
from minio import Minio
import pandas as pd

from utils.data_constants import BUCKET_NAMES,DATASET_FILEPATH
from utils.minio_utils import create_bucket

# def connect_to_client():
#     client = Minio('127.0.0.1:9000',access_key = 'minioadmin', secret_key = 'minioadmin',secure=False)
#     return client

def preprocess(client,chunk, index):
    json = chunk.to_json(orient='records')
    # Upload data with content-type as application/json
    client.put_object('testbucket', f"batch_{index}", io.BytesIO(bytes(json,'ascii')), len(bytes(json,'ascii')), content_type="application/json")

def main():
    client = create_bucket('testbucket')
    df = pd.read_csv(DATASET_FILEPATH, chunksize=100)
    [preprocess(client,data, index) for index,data in enumerate(df)]


if __name__ == "__main__":
    main()