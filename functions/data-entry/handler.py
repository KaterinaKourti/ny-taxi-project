from dotenv import load_dotenv
from json import loads,dumps
import pandas as pd
from minio import Minio
from os import getcwd,environ
from os.path import join,dirname
import io
from termcolor import colored

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

BUCKET_NAMES = {
    'data_entry': 'data-entry'
}

DATASET_FILEPATH = getcwd()+'/data/test.csv'

def create_bucket(bucket,verbose=True):

    HOST_IP = environ.get("HOST_IP")
    PORT = environ.get("PORT")
    HOSTNAME = HOST_IP+':'+PORT
    ACCESS_KEY = environ.get("ACCESS_KEY")
    SECRET_KEY = environ.get("SECRET_KEY")
    client = Minio(HOSTNAME,access_key = ACCESS_KEY, secret_key = SECRET_KEY,secure=False)
    
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    if verbose:
        buckets = client.list_buckets()
        for b in buckets:
            print(b.name, b.creation_date)

    return client


def data_entry(client,chunk,index):
    # Upload data with content-type as application/json
    csv = chunk.to_csv(index=False)
    client.put_object(BUCKET_NAMES['data_entry'], f"batch_{index}", io.BytesIO(bytes(csv,'ascii')), len(bytes(csv,'ascii')), content_type="application/csv")

def handle():

    error = ''
    try:
        # data_entry_url = 'http://127.0.0.1:8080/function/data_entry'
        data_entry_bucket = BUCKET_NAMES['data_entry']
        client = create_bucket(data_entry_bucket)
        dfs = pd.read_csv(DATASET_FILEPATH,chunksize=100)
        [data_entry(client,data, index) for index,data in enumerate(dfs)]
    except Exception as e:
        error = e
        print(colored("Error in data-entry handler",'red'))

    payload = { 'success': False, 'error': error } if error else { 'success': True, 'message': f'Entry data have been successfully uploaded to Minio in bucket {data_entry_bucket}' }

    for key, value in payload.items():
        print(colored(f"{key}: {value}",'cyan'))
        print()

    
if __name__ == "__main__":
    handle()