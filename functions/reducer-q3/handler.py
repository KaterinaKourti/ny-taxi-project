from functools import reduce
import io
from json import loads,dumps
from minio import Minio
from os import environ


BUCKET_NAMES = {
    'reducer-q3': 'reducer-data-q3',
    'final-q3': 'final-results-q3'
}

def connect_to_minio():
    client = Minio(environ.get("MINIO_HOSTNAME"),access_key = environ.get("MINIO_ACCESS_KEY"), secret_key = environ.get("MINIO_SECRET_KEY"),secure=False)
    return client


def sum_mapped_data(batch):

    payload = {
        'NE': 0,
        'NW': 0,
        'SE': 0,
        'SW': 0
    }
    
    for key,value in batch.items():
        # print(type(value))
        sum = 0
        if value: sum = reduce(lambda cur, acc: cur + acc,value)
        payload.update({key: sum})

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
    


def reducer(client,bucket,index,batch):

    # Query 3 Logic
    reduced_data = get_optimal_quarter(batch)
    data = dumps(reduced_data)
    # print(colored(mapped_data,'cyan'))

    client.put_object(bucket, f"Q3_reducer_results_batch_{index}", io.BytesIO(bytes(data,'ascii')), len(bytes(data,'ascii')), content_type="application/json")
    
    


def handle(req):
    error = ''
    try:
        body = loads(req)
        # preprocess_url = 'http://127.0.0.1:8080/function/preprocess'
        batch_name = body['Key'].split('/')[1]
        index = batch_name.split('_')[-1]
        
        reducer_bucket = BUCKET_NAMES['reducer-q3']
        final_bucket = BUCKET_NAMES['final-q3']
        client = connect_to_minio()
        print('Batch Name: ',batch_name)
        batch = client.get_object(reducer_bucket,batch_name).read().decode('utf-8')
        # we must decode the byte array into str and then load it as json
        json_batch = loads(batch)
        # print(batches)
        reducer(client,final_bucket,index,json_batch)
    except Exception as e:
        error = e
        print("Error in reducer-q3 handler")

    payload = { 'success': False, 'error': error } if error else { 'success': True, 'message': f'Reducer data for q3 have been successfully uploaded to Minio in bucket {reducer_bucket}' }

    return payload
