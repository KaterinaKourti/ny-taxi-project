import argparse
from functools import reduce
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


def sum_mapped_data(batch):

    payload = {
        'NE': 0,
        'NW': 0,
        'SE': 0,
        'SW': 0
    }

    print(colored(batch,'red'))
    
    for key,value in batch.items():
        # print(type(value))
        sum = 0
        if value: sum = reduce(lambda cur, acc: cur + acc,value)
        payload.update({key: sum})

    return payload


def get_effective_trips(batch):

    payload = {
        "effective_trip_ids": batch['effective']
    }

    return payload

def get_optimal_quarter(batch):

    sum_data = sum_mapped_data(batch)

    payload = {
        "optimal_quarter": max(sum_data, key=sum_data.get),
        "max_demand": 0
    }
    
    payload.update({"max_demand": sum_data[payload['optimal_quarter']]})
    print(payload)
    return payload
    


def reducer(client,bucket,index,batch,query):

    if query == 1:
        # Query 1 Logic
        reduced_data = sum_mapped_data(batch)
        # print(reduced_data)
        data = dumps(reduced_data)
        client.put_object(bucket, f"Q1_reducer_results_batch_{index}", io.BytesIO(bytes(data,'ascii')), len(bytes(data,'ascii')), content_type="application/json")
        
    elif query == 2:
        # Query 2 Logic
        reduced_data = get_effective_trips(batch)
        data = dumps(reduced_data)
        client.put_object(bucket, f"Q2_reducer_results_batch_{index}", io.BytesIO(bytes(data,'ascii')), len(bytes(data,'ascii')), content_type="application/json")
        
    
    else:
        
        # Query 3 Logic
        reduced_data = get_optimal_quarter(batch)
        data = dumps(reduced_data)
        # print(colored(mapped_data,'cyan'))

        client.put_object(bucket, f"Q3_reducer_results_batch_{index}", io.BytesIO(bytes(data,'ascii')), len(bytes(data,'ascii')), content_type="application/json")
        
    


def main(args):
    query = args.query
    mapper_bucket = BUCKET_NAMES['mapper'].format(f'q{query}')
    reducer_bucket = BUCKET_NAMES['reducer'].format(f'q{query}')
    client = create_bucket(reducer_bucket)
    # batches = get_batches(client,BUCKET_NAMES['preprocess'])
    batches = get_batches(client,mapper_bucket) # test 
    # !!! we must decode the byte array into str and then load it as json
    json_batches = [loads(batch.decode('utf-8')) for batch in batches]
    # print(batches)
    [reducer(client,reducer_bucket,index,batch,query) for index,batch in enumerate(json_batches)]

if __name__ == "__main__":
    main(args)