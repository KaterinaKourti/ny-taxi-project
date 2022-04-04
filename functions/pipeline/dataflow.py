import argparse
import requests
from minio import Minio
from termcolor import colored
from json import dumps

parser = argparse.ArgumentParser()
parser.add_argument("-query", default = 1, type = int)
args = parser.parse_args()

def invoke(query):

    bucket_name = 'data-entry'
    client = Minio('127.0.0.1:9000',access_key = 'digitalinfra', secret_key = '$JJyaQxS&dW7Y2eF',secure=False)
    objects = client.list_objects(bucket_name)

    print(colored(f"Objects: {objects}",'yellow'))

    url = 'http://127.0.0.1:8080/function/preprocess'
    
    def send_request(object):

        print(colored(f"Object: {object.object_name}",'blue'))

        body = {
            "query": query,
            "batch_info": f'{bucket_name}/{object.object_name}'
        }
        for key, value in body.items():
            print(f"{key}: {value}")

        req = requests.post(url,json=body)
        print(req.status_code)

    list(map(lambda x: send_request(x),objects))
 
def main(args):
    query = args.query
    invoke(query)

if __name__ == "__main__":
    main(args)