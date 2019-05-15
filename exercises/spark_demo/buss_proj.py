from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import *

from exercises.catalog.catalog import file_to_frame
from pathlib import Path

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    flights_frame = file_to_frame("cleaned_flight2000", spark)
    carriers_frame = file_to_frame("cleaned_carriers", spark)
    airports_frame = file_to_frame("cleaned_airports", spark)


    # flights_frame.show(15)
    flights_frame.printSchema()
    carriers_frame.printSchema()
    airports_frame.printSchema()

    combined_frame1 = (flights_frame
                       .join(carriers_frame,
                             (flights_frame['UniqueCarrier'] == carriers_frame['Code']),
                             "inner")
                       )

    combined_frame2 = (combined_frame1
                       .join(airports_frame,
                             (combined_frame1['Origin'] == airports_frame['Airport_Code']),
                             "inner")
                       .withColumnRenamed('Airport_Code', 'Airport_Origin_Code')
                       .withColumnRenamed('Airport_Description', 'Airport_Origin_Description')
                       )

    # combined_frame2.show(25)
    combined_frame3 = (combined_frame2
                       .join(airports_frame,
                            (combined_frame2['Dest'] == airports_frame['Airport_Code']),
                            "inner")
                       .withColumnRenamed('Airport_Code', 'Airport_Dest_Code')
                       .withColumnRenamed('Airport_Description', 'Airport_Dest_Description')
                       )
    #combined_frame3.cache()
    #combined_frame3.show(10)
    target_dir = Path(__file__).parents[1] / "target"
    target_dir.mkdir(exist_ok=True)
    combined_frame3.write.mode("overwrite").parquet(str(target_dir / "combined_flights"))
