from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pipelines.util.platform import start_spark
from pipelines.util.storage import read_csv,read_csvfolder,write_partby_parquet,read_parquet_filter
from pipelines.metadata import MetaData

# Function to extract data from specific date
def extract_wtdata(sps: SparkSession) -> DataFrame:
    
    sch = T.StructType([
        T.StructField("timestamp", T.TimestampType(), True),
        T.StructField("turbine_id", T.IntegerType(), True),
        T.StructField("wind_speed", T.DoubleType(), True),
        T.StructField("wind_direction", T.IntegerType(), True),
        T.StructField("power_output", T.DoubleType(), True)
    ])
    csvoptions = {
        "header": "true",
        "sep": ",",
        "schema": sch,
        "inferSchema": "false"
    }
    df_wtdata = read_csvfolder(sps, "{storage_path}".format(storage_path = sps.conf.get('storage.landed')), csvoptions)

    df_wtdata = df_wtdata.withColumn("outputdate",F.to_date(F.col("timestamp")))

    write_partby_parquet(sps, df_wtdata, ["outputdate","turbine_id"], "{storage_path}".format(storage_path = sps.conf.get('storage.extracted')))

def get_latestextract_wtdata(sps: SparkSession, md: MetaData, pqoptions) -> DataFrame:

    df_ext_wtdata = read_parquet_filter(sps, F.col("outputdate") > md.watermark_date, "{storage_path}".format(storage_path = sps.conf.get('storage.extracted')), pqoptions)

    return df_ext_wtdata
