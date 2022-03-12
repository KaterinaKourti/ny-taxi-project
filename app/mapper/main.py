import argparse
from minio import Minio
import pandas as pd
from utils.data_constants import *
from utils.minio_utils import create_bucket
 
parser = argparse.ArgumentParser()
parser.add_argument("-query", default = 1, type = int)
args = parser.parse_args()



def get_batches(client,bucket_name):
    batches = client.list_objects(bucket_name)
    return batches

    
def get_pickup_coordinates(batch):
    lat = batch['pickup_latitude']
    long = batch['pickup_latitude']
    return lat, long

    # ['NW',1]
    # ['NW',[1,1,1,1,1]]

def mapper(batch, query):

    

    if query == 1:
        # Query 1 Logic
        return

    elif query == 2:
        # Query 2 Logic
        return 
    
    else:
        return 
        # Query 3 Logic





def main(args):
    query = args.query
    mapper_bucket = BUCKET_NAMES['mapper']
    client = create_bucket(mapper_bucket)
    batches = get_batches(client,BUCKET_NAMES['preprocess'])
    [mapper(batch,query) for batch in batches]