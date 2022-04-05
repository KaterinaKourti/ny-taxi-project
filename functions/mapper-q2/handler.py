import datetime
import haversine as hs
import io
from json import loads, dumps
from minio import Minio
from os import environ

BUCKET_NAMES = {"mapper-q2": "mapper-data-q2", "reducer-q2": "reducer-data-q2"}

def connect_to_minio():
    client = Minio(
        environ.get("MINIO_HOSTNAME"),
        access_key=environ.get("MINIO_ACCESS_KEY"),
        secret_key=environ.get("MINIO_SECRET_KEY"),
        secure=False,
    )
    return client


def calculate_distance(row):
    # Get Pickup (lat,long) coordinates from each row of the batch and turn them into point -> (lat,long) tuple
    source_lat = float(row["pickup_latitude"])
    source_long = float(row["pickup_longitude"])
    source_point = (source_lat, source_long)
    # Get Dropoff (lat,long) coordinates from each row of the batch and turn them into point -> (lat,long) tuple
    dest_lat = float(row["dropoff_latitude"])
    dest_long = float(row["dropoff_longitude"])
    dest_point = (dest_lat, dest_long)

    distance_in_km = hs.haversine(source_point, dest_point)
    return distance_in_km


def trip_stats(row):
    trip_distance = round(calculate_distance(row), 1)
    trip_duration = int(row["trip_duration"])  # in secs
    passenger_count = int(row["passenger_count"])
    trip_id = row["id"]
    if trip_distance > 1.0 and trip_duration > 600 and passenger_count > 2:
        return ["effective", trip_id]
    else:
        return ["ineffective", trip_id]


def shuffle_trip_stats(batch):
    trips = {"effective": [], "ineffective": []}
    for trip in batch:
        if trip[0] == "effective":
            trips["effective"].append(trip[1])
        elif trip[0] == "ineffective":
            trips["ineffective"].append(trip[1])
    return trips


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
    mapped_data = shuffle_trip_stats(list(map(lambda row: trip_stats(row), batch)))
    data = dumps(mapped_data)
    response = client.put_object(
        bucket,
        f"Q2_mapper_results_batch_{index}",
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
        mapper_bucket = BUCKET_NAMES["mapper-q2"]
        reducer_bucket = BUCKET_NAMES["reducer-q2"]
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
