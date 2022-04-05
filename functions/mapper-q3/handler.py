import datetime
import io
from minio import Minio
from json import loads, dumps
from os import environ

NY_BASE_LAT = 40.730610
NY_BASE_LONG = -73.935242

BUCKET_NAMES = {"mapper-q3": "mapper-data-q3", "reducer-q3": "reducer-data-q3"}

def connect_to_minio():
    client = Minio(
        environ.get("MINIO_HOSTNAME"),
        access_key=environ.get("MINIO_ACCESS_KEY"),
        secret_key=environ.get("MINIO_SECRET_KEY"),
        secure=False,
    )
    return client

def find_quarter(row):
    lat = float(row["pickup_latitude"])
    long = float(row["pickup_longitude"])
    if lat > NY_BASE_LAT and long > NY_BASE_LONG:
        return ["NE", 1]
    elif lat > NY_BASE_LAT and long < NY_BASE_LONG:
        return ["NW", 1]
    elif lat < NY_BASE_LAT and long > NY_BASE_LONG:
        return ["SE", 1]
    elif lat < NY_BASE_LAT and long < NY_BASE_LONG:
        return ["SW", 1]


def shuffle_quarters(batch):
    quarters = {"NE": [], "NW": [], "SE": [], "SW": []}
    for row in batch:
        if row[0] == "NE":
            quarters["NE"].append(1)
        elif row[0] == "NW":
            quarters["NW"].append(1)
        elif row[0] == "SE":
            quarters["SE"].append(1)
        elif row[0] == "SW":
            quarters["SW"].append(1)
    return quarters


def optimal_trips(row):
    date = row["pickup_datetime"]
    date_formatted = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    week_day = date_formatted.isoweekday()
    time = date_formatted.time()
    lower_morning_rush = datetime.time(7, 0, 0)
    upper_morning_rush = datetime.time(10, 0, 0)
    lower_noon_rush = datetime.time(15, 0, 0)
    upper_noon_rush = datetime.time(19, 0, 0)
    if week_day in [1, 5] and (
        lower_morning_rush <= time <= upper_morning_rush
        or lower_noon_rush <= time <= upper_noon_rush
    ):
        quarter = find_quarter(row)
        return quarter

def mapper(client, bucket, index, batch):
    optimals = [optimal_trips(x) for x in batch if optimal_trips(x) is not None]
    mapped_data = shuffle_quarters(optimals)
    data = dumps(mapped_data)
    response = client.put_object(
        bucket,
        f"Q3_mapper_results_batch_{index}",
        io.BytesIO(bytes(data, "ascii")),
        len(bytes(data, "ascii")),
        content_type="application/json",
    )

def handle(req):
    error = ""
    try:
        body = loads(req)
        batch_name = body["Key"].split("/")[1]
        index = batch_name.split("_")[-1]
        mapper_bucket = BUCKET_NAMES["mapper-q3"]
        reducer_bucket = BUCKET_NAMES["reducer-q3"]
        client = connect_to_minio()
        print("Batch Name: ", batch_name)
        # we must decode the byte array into str and then load it as json
        batch = client.get_object(mapper_bucket, batch_name).read().decode("utf-8")
        json_batch = loads(batch)
        mapper(client, reducer_bucket, index, json_batch)
    except Exception as e:
        error = e
        print("Error in mapper-q2 handler")
    payload = (
        {"success": False, "error": error}
        if error
        else {
            "success": True,
            "message": f"Mapper data for q2 have been successfully uploaded to Minio in bucket {mapper_bucket}",
        }
    )
    return payload
