import datetime
import io
from json import loads,dumps
from minio import Minio
from os import environ


def connect_to_minio():
    client = Minio(environ.get("MINIO_HOSTNAME"),access_key = environ.get("MINIO_ACCESS_KEY"), secret_key = environ.get("MINIO_SECRET_KEY"),secure=False)
    return client


NY_BASE_LAT = 40.730610
NY_BASE_LONG = -73.935242

BUCKET_NAMES = {
    'mapper-q1': 'mapper-data-q1',
    'reducer-q1': 'reducer-data-q1'
}

def find_quarter(row):

    lat = float(row['pickup_latitude'])
    long = float(row['pickup_longitude'])

    if lat > NY_BASE_LAT and long > NY_BASE_LONG:
        return ['NE',1]
    elif lat > NY_BASE_LAT and long < NY_BASE_LONG:
        return ['NW',1]
    elif lat < NY_BASE_LAT and long > NY_BASE_LONG:
        return ['SE',1]
    elif lat < NY_BASE_LAT and long < NY_BASE_LONG:
        return ['SW',1]
    
def shuffle_quarters(batch):

    quarters = {
        'NE': [],
        'NW': [],
        'SE': [],
        'SW': []
    }

    for row in batch:
        if row[0] == "NE":
            quarters['NE'].append(1)
        elif row[0] == "NW":
            quarters['NW'].append(1)
        elif row[0] == "SE":
            quarters['SE'].append(1)
        elif row[0] == "SW":
            quarters['SW'].append(1)

    return quarters



def mapper(client,bucket,index,batch):
    # Query 1 Logic
    mapped_data = shuffle_quarters(list(map(lambda row: find_quarter(row), batch)))
    data = dumps(mapped_data)
    response = client.put_object(bucket, f"Q1_mapper_results_batch_{index}", io.BytesIO(bytes(data,'ascii')), len(bytes(data,'ascii')), content_type="application/json")
    


def handle(req):
    error = ''
    try:
        body = loads(req)
        # preprocess_url = 'http://127.0.0.1:8080/function/preprocess'
        batch_name = body['Key'].split('/')[1]
        index = batch_name.split('_')[-1]
        mapper_bucket = BUCKET_NAMES['mapper-q1']
        reducer_bucket = BUCKET_NAMES['reducer-q1']
        client = connect_to_minio()
        print('Batch Name: ',batch_name)
        batch = client.get_object(mapper_bucket,batch_name).read().decode('utf-8')
        # we must decode the byte array into str and then load it as json
        json_batch = loads(batch)
        # print(batches)
        mapper(client,reducer_bucket,index,json_batch)

    except Exception as e:
        error = e
        print("Error in mapper-q1 handler")

    payload = { 'success': False, 'error': error } if error else { 'success': True, 'message': f'Mapper data for q1 have been successfully uploaded to Minio in bucket {reducer_bucket}' }

    return payload
    # print(os.environ.get("MINIO_HOSTNAME"))

