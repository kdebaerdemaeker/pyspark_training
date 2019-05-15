from pathlib import Path
from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import *

from exercises.catalog.catalog import file_to_frame

def clean(frame: DataFrame) -> DataFrame:
    # First, get the majority of columns “fixed”, i.e. their datatypes improved.
    df2 = (frame
           .withColumnRenamed('Code', 'Airport_Code')
           .withColumnRenamed('Description', 'Airport_Description')
           )
    return df2

if __name__ == "__main__":
    target_dir = Path(__file__).parents[1] / "target"
    target_dir.mkdir(exist_ok=True)
    spark = SparkSession.builder.getOrCreate()
    frame = file_to_frame("airports",spark)

    # frame.show(15)

    cleaned_frame=clean(frame)
    cleaned_frame.printSchema()
    cleaned_frame.write.mode("overwrite").parquet(str(target_dir / "cleaned_airports"))