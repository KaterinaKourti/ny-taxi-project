import argparse
import datetime
import haversine as hs
import io
from json import loads,dumps
from termcolor import colored

from utils.data_constants import *
from utils.minio_utils import create_bucket
 
parser = argparse.ArgumentParser()
parser.add_argument("-query", default = 3, type = int)
args = parser.parse_args()



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


def calculate_distance(row):
    # Get Pickup (lat,long) coordinates from each row of the batch and turn them into point -> (lat,long) tuple
    source_lat = float(row['pickup_latitude'])
    source_long = float(row['pickup_longitude'])
    source_point = (source_lat,source_long)

    # Get Dropoff (lat,long) coordinates from each row of the batch and turn them into point -> (lat,long) tuple
    dest_lat = float(row['dropoff_latitude'])
    dest_long = float(row['dropoff_longitude'])
    dest_point = (dest_lat,dest_long)

    distance_in_km = hs.haversine(source_point,dest_point)

    return distance_in_km

def trip_stats(row):
    trip_distance = round(calculate_distance(row),1)
    trip_duration = int(row['trip_duration']) # in secs
    passenger_count = int(row['passenger_count'])
    trip_id = row['id']

    if trip_distance > 1.0 and trip_duration > 600 and passenger_count > 2:
        return ['effective',trip_id]
    else:
        return ['ineffective',trip_id]


def shuffle_trip_stats(batch):
    trips = {
        "effective":[],
        "ineffective":[]
    }

    for trip in batch:
        if trip[0] == 'effective':
            trips['effective'].append(trip[1])
        elif trip[0] == 'ineffective':
            trips['ineffective'].append(trip[1])
    
    return trips


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
        



def mapper(client,bucket,index,batch,query):

    if query == 1:
        # Query 1 Logic
        mapped_data = shuffle_quarters(list(map(lambda row: find_quarter(row), batch)))
        data = dumps(mapped_data)
        response = client.put_object(bucket, f"Q1_mapper_results_batch_{index}", io.BytesIO(bytes(data,'ascii')), len(bytes(data,'ascii')), content_type="application/json")
        
    elif query == 2:
        # Query 2 Logic
        mapped_data = shuffle_trip_stats(list(map(lambda row: trip_stats(row), batch)))
        data = dumps(mapped_data)
        response = client.put_object(bucket, f"Q2_mapper_results_batch_{index}", io.BytesIO(bytes(data,'ascii')), len(bytes(data,'ascii')), content_type="application/json")
        
    
    else:
        
        # Query 3 Logic
        # optimals = list(map(lambda row: optimal_trips(row) , batch))
        # optimals_filtered = list(filter(lambda x: x is not None, optimals))
        optimals = [optimal_trips(x) for x in batch if optimal_trips(x) is not None]
        # print(optimals)
        mapped_data = shuffle_quarters(optimals)
        data = dumps(mapped_data)
        # print(colored(mapped_data,'cyan'))

        response = client.put_object(bucket, f"Q3_mapper_results_batch_{index}", io.BytesIO(bytes(data,'ascii')), len(bytes(data,'ascii')), content_type="application/json")
        
    


def main(args):
    query = args.query
    mapper_bucket = BUCKET_NAMES['mapper'].format(f'q{query}')
    print(mapper_bucket)
    client = create_bucket(mapper_bucket)
    # batches = get_batches(client,BUCKET_NAMES['preprocess'])
    batches = get_batches(client,'testbucket') # test 
    # !!! we must decode the byte array into str and then load it as json
    json_batches = [loads(batch.decode('utf-8')) for batch in batches]
    # print(batches)
    [mapper(client,mapper_bucket,index,batch,query) for index,batch in enumerate(json_batches)]

if __name__ == "__main__":
    main(args)