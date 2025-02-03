from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import date
import io

class MetaData:

    min_wind_speed = 9.0
    max_wind_speed = 15.0
    min_power_output = 1.5
    max_power_output = 4.5
    min_wind_direction = 0
    max_wind_direction = 359

    watermark_date = "1900-01-01"

    def __init__(self):
        pass

    def get_watermark_date(self, sps: SparkSession) -> str:
        
        try:
            with open("{storage_path}/watermark.txt".format(storage_path = sps.conf.get('storage.metadata')), "r") as f:
                self.watermark_date = f.read().strip()
        except:
            self.watermark_date = "1900-01-01"
        
        return self.watermark_date or "1900-01-01"
    
    def set_watermark_date(self, sps: SparkSession, wtdata: DataFrame):

        watermark_date = wtdata.agg(F.max(F.col("outputdate")).alias("watermark_date")).first()["watermark_date"]

        with open("{storage_path}/watermark.txt".format(storage_path = sps.conf.get('storage.metadata')), "w") as f:
            f.write(watermark_date.strftime("%Y-%m-%d"))

    