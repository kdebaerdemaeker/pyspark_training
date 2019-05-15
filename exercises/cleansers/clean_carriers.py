from pathlib import Path
from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import *

from exercises.catalog.catalog import file_to_frame

def clean(frame: DataFrame) -> DataFrame:
    # First, get the majority of columns “fixed”, i.e. their datatypes improved.
    df2 = (frame
           .withColumn('Carrier_Code', col('Code'))
           .withColumn('Carrier_Description', col('Description'))
           )
    return df2

if __name__ == "__main__":
    target_dir = Path(__file__).parents[1] / "target"
    target_dir.mkdir(exist_ok=True)
    spark = SparkSession.builder.getOrCreate()
    frame = file_to_frame("carriers",spark)
    #frame.describe()
    #frame.show(15)
    cleaned_frame=clean(frame)
    cleaned_frame.write.mode("overwrite").parquet(str(target_dir / "cleaned_carriers"))