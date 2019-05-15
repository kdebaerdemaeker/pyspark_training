import pytest
import datetime as dt
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, IntegerType, StructType,DateType, BooleanType
from pyspark.sql.functions import dayofweek

spark = SparkSession.builder.master("local[*]").getOrCreate()

fields = [StructField("date", DateType(), True)]
vandaag = dt.date.today()
[vandaag + dt.timedelta(days=d) for d in range(10)]


frame = spark.createDataFrame(
    [(vandaag + dt.timedelta(days=d),) for d in range(10)],
    schema=StructType(fields))
frame.show()
frame.printSchema()

