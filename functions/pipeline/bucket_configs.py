import argparse
from minio import Minio

parser = argparse.ArgumentParser()
parser.add_argument("-query", default=1, type=int)
args = parser.parse_args()

BUCKET_CONFIGS = {
    "1": {
        "mapper": "mapper-data-q1",
        "reducer": "reducer-data-q1",
        "final": "final-results-q1",
    },
    "2": {
        "mapper": "mapper-data-q2",
        "reducer": "reducer-data-q2",
        "final": "final-results-q2",
    },
    "3": {
        "mapper": "mapper-data-q3",
        "reducer": "reducer-data-q3",
        "final": "final-results-q3",
    },
}


def create_buckets(args):
    query = args.query
    client = Minio(
        "127.0.0.1:9000",
        access_key="digitalinfra",
        secret_key="$JJyaQxS&dW7Y2eF",
        secure=False,
    )

    def create_bucket(bucket, verbose=True):
        if verbose:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)

            buckets = client.list_buckets()
            for b in buckets:
                print(b.name, b.creation_date)

    query_dict = BUCKET_CONFIGS[f"{query}"]
    list(map(lambda v: create_bucket(v), query_dict.values()))


if __name__ == "__main__":
    create_buckets(args)
