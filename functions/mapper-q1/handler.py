import datetime
import io
from json import loads,dumps
from termcolor import colored
from minio import Minio

def create_bucket(bucket,verbose=False):
    client = Minio('192.168.68.108:9000',access_key = 'minioadmin', secret_key = 'minioadmin',secure=False)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    if verbose:
        buckets = client.list_buckets()
        for b in buckets:
            print(b.name, b.creation_date)

    return client


NY_BASE_LAT = 40.730610
NY_BASE_LONG = -73.935242

BUCKET_NAMES = {
    'data-entry': 'data-entry',
    'preprocess': 'batches',
    'mapper-q1': 'mapper-data-q1',
    'mapper-q2': 'mapper-data-q2',
    'mapper-q3': 'mapper-data-q3',
    'reducer-q1': 'reducer-data-q1',
    'reducer-q2': 'reducer-data-q2',
    'reducer-q3': 'reducer-data-q3'
}



def get_batches(client,bucket_name):
    objects = client.list_objects(bucket_name)
    batches = []
    for obj in objects:
        response = client.get_object(bucket_name,obj.object_name)
        batches.append(response.read())
        response.close()
        response.release_conn()
    return batches


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
        mapper_bucket = BUCKET_NAMES['mapper-q1']
        client = create_bucket(mapper_bucket)
        # batches = get_batches(client,BUCKET_NAMES['preprocess'])
        batches = get_batches(client,'testbucket') # test 
        # we must decode the byte array into str and then load it as json
        json_batches = [loads(batch.decode('utf-8')) for batch in batches]
        # print(batches)
        [mapper(client,mapper_bucket,index,batch) for index,batch in enumerate(json_batches)]

    except Exception as e:
        error = e
        print(colored("Error in mapper-q1 handler",'red'))

    payload = { 'success': False, 'error': error } if error else { 'success': True, 'message': f'Mapper data for q1 have been successfully uploaded to Minio in bucket {mapper_bucket}' }

    return payload

