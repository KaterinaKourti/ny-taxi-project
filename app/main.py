import pandas as pd
from minio import Minio

csv_file_path = "data/fares.csv"

#------------------------------------------------------- Preprocess ---------------------------------------------------------------

def preprocess(chunk, index):
    json = chunk.to_json(orient='records')
    client = Minio("play.min.io")
    # Upload data with content-type.
    result = client.put_object("batches", f"batch_{index}", chunk, -1, content_type="application/json")

def main():
    df = pd.read_csv(csv_file_path, chunksize=100)
    [preprocess(data, index) for data, index in enumerate(df)]

#------------------------------------------------------- Question 1 ---------------------------------------------------------------

questionEnum = {
    'QUESTION_1': 1,
    'QUESTION_2': 2
}

def getChunk():


def chaining(chunk, selectedQuerstion):
    if (selectedQuerstion)

#------------------------------------------------------- Question 2 ---------------------------------------------------------------

# class NoRatings(MRJob):
#     def steps(self):
#         return[
#             MRStep(mapper=self.mapper_get_ratings,
#             reducer=self.reducer_count_ratings)
#         ]
# #Mapper function 
#     def mapper_get_ratings(self, _, line):
#         print(list(map(json_file_path), filter(lambda x: x.trip_duration>600 and x.passenger_count>2)))
# #Reducer function
#     def reducer_count_ratings(self, key, values):
#         print(self)
#------------------------------------------------------- Question 3 ---------------------------------------------------------------
# NoRatings.run()