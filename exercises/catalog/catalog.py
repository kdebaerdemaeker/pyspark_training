from pathlib import Path
from pyspark.sql import SparkSession

exercise_path = (Path(__file__).parents[1])
# print (catalog_path)

catalog = {
    "flight2000": (exercise_path / "resources/flights/2000.csv", "csv", dict(header="true", sep=",")),
    "flight2008": (exercise_path / "resources/flights/2008.csv", "csv", dict(header="true", sep=",")),
    "airports": (exercise_path / "resources/flights/airports.csv", "csv", dict(header="true", sep=",")),
    "carriers": (exercise_path / "resources/flights/carriers.csv", "csv", dict(header="true", sep=",")),
    "cleaned_flight2000": (exercise_path / "target/cleaned_flights2000", "parquet", dict()),
    "cleaned_flight2008": (exercise_path / "target/cleaned_flights2008", "parquet", dict()),
    "cleaned_airports": (exercise_path / "target/cleaned_airports", "parquet", dict()),
    "cleaned_carriers": (exercise_path / "target/cleaned_carriers", "parquet", dict())
}


# for key in catalog:
#     print (key)
#     print (catalog(key))

def file_to_frame(catalog_entry, session: SparkSession):
    spark = session
    meta = catalog[catalog_entry]
    path = str(meta[0])
    filetype = meta[1]
    frame = spark.read.format(filetype).load(path, **meta[2])
    return frame
