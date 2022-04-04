from functools import reduce
import io
from json import loads,dumps
from minio import Minio
import os

BUCKET_NAMES = {
    'reducer-q1': 'reducer-data-q1',
    'final-q1': 'final-results-q1'
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


def reducer(client,bucket,index,batch):

    # Query 1 Logic
    reduced_data = sum_mapped_data(batch)
    # print(reduced_data)
    data = dumps(reduced_data)
    client.put_object(bucket, f"Q1_reducer_results_batch_{index}", io.BytesIO(bytes(data,'ascii')), len(bytes(data,'ascii')), content_type="application/json")
    
    


def handle(req):
    error = ''
    try:
        body = loads(req)
        # preprocess_url = 'http://127.0.0.1:8080/function/preprocess'
        batch_name = body['Key'].split('/')[1]
        index = batch_name.split('_')[-1]
        
        reducer_bucket = BUCKET_NAMES['reducer-q1']
        final_bucket = BUCKET_NAMES['final-q1']
        client = connect_to_minio()
        print('Batch Name: ',batch_name)
        batch = client.get_object(reducer_bucket,batch_name).read().decode('utf-8')
        # we must decode the byte array into str and then load it as json
        json_batch = loads(batch)
        # print(batches)
        reducer(client,final_bucket,index,json_batch)
    except Exception as e:
        error = e
        print("Error in reducer-q1 handler")

    payload = { 'success': False, 'error': error } if error else { 'success': True, 'message': f'Reducer data for q1 have been successfully uploaded to Minio in bucket {final_bucket}' }

    return payload

    
    

