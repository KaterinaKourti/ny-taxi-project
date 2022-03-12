from minio import Minio

def create_bucket(bucket):
    client = Minio('127.0.0.1:9000',access_key = 'minioadmin', secret_key = 'minioadmin',secure=False)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    buckets = client.list_buckets()
    for b in buckets:
        print(b.name, b.creation_date)

    return client