import datetime
import io
from minio import Minio
from json import loads,dumps
from termcolor import colored

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

def create_bucket(bucket,verbose=False):
    client = Minio('192.168.68.108:9000',access_key = 'minioadmin', secret_key = 'minioadmin',secure=False)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    if verbose:
        buckets = client.list_buckets()
        for b in buckets:
            print(b.name, b.creation_date)

    return client

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
 

def optimal_trips(row):
    date = row['pickup_datetime']
    date_formatted = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    week_day = date_formatted.isoweekday()
    # time = datetime.time(date_formatted.hour,date_formatted.minute, date_formatted.second)
    time = date_formatted.time()
    lower_morning_rush = datetime.time(7, 0, 0)
    upper_morning_rush = datetime.time(10, 0, 0)
    lower_noon_rush = datetime.time(15, 0, 0)
    upper_noon_rush = datetime.time(19, 0, 0)


    # print(colored(f"Weekday: {week_day}",'blue'))
    # print(colored(f"Time: {time}",'yellow'))
    # print(colored(f"Lower morning rush: {lower_morning_rush}",'magenta'))

    if week_day in [1,5] and (lower_morning_rush <= time <= upper_morning_rush or lower_noon_rush <= time <= upper_noon_rush):
        quarter = find_quarter(row)
        # print(colored(quarter,'magenta'))
        return quarter
        



def mapper(client,bucket,index,batch):

    # Query 3 Logic
    # optimals = list(map(lambda row: optimal_trips(row) , batch))
    # optimals_filtered = list(filter(lambda x: x is not None, optimals))
    optimals = [optimal_trips(x) for x in batch if optimal_trips(x) is not None]
    # print(optimals)
    mapped_data = shuffle_quarters(optimals)
    data = dumps(mapped_data)
    # print(colored(mapped_data,'cyan'))

    response = client.put_object(bucket, f"Q3_mapper_results_batch_{index}", io.BytesIO(bytes(data,'ascii')), len(bytes(data,'ascii')), content_type="application/json")
    
    


def handle(req):

    error = ''
    try:
        mapper_bucket = BUCKET_NAMES['mapper-q3']
        client = create_bucket(mapper_bucket)
        # batches = get_batches(client,BUCKET_NAMES['preprocess'])
        batches = get_batches(client,'testbucket') # test 
        # we must decode the byte array into str and then load it as json
        json_batches = [loads(batch.decode('utf-8')) for batch in batches]
        # print(batches)
        [mapper(client,mapper_bucket,index,batch) for index,batch in enumerate(json_batches)]

    except Exception as e:
        error = e
        print(colored("Error in mapper-q2 handler",'red'))

    payload = { 'success': False, 'error': error } if error else { 'success': True, 'message': f'Mapper data for q2 have been successfully uploaded to Minio in bucket {mapper_bucket}' }

    return payload