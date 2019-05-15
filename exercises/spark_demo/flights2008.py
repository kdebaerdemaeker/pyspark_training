from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import *

from exercises.catalog.catalog import file_to_frame
from pathlib import Path

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    flights_frame = file_to_frame("cleaned_flight2008", spark)
    # flights_frame.show(15)
    flights_frame.printSchema()
    print(flights_frame.count())
    done_flights_frame = flights_frame.filter(flights_frame.Cancelled == False)
    print(done_flights_frame.count())




