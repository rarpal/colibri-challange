from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pipelines.util.platform import start_spark
from pipelines.util.storage import read_csv,read_csvfolder,write_partby_parquet,write_parquet,read_parquet_filter,read_parquet

def load_wtdata(sps: SparkSession):

    sch = T.StructType([
        T.StructField("timestamp", T.TimestampType(), True),
        T.StructField("turbine_id", T.IntegerType(), True),
        T.StructField("wind_speed", T.DoubleType(), True),
        T.StructField("wind_direction", T.IntegerType(), True),
        T.StructField("power_output", T.DoubleType(), True),
        T.StructField("outputdate", T.DateType(), True),
        T.StructField("is_missing", T.BooleanType(), True),
        T.StructField("wind_speed_cleaned", T.DoubleType(), True),
        T.StructField("power_output_cleaned", T.DoubleType(), True),
        T.StructField("wind_direction_cleaned", T.IntegerType(), True)
    ])
    pqoptions = {
        "schema": sch,
        "inferSchema": "false"
    }

    wtdata_validated = get_latestvalidated_wtdata(sps, pqoptions)

    wtdata_validated.createOrReplaceTempView("wtdata_validated")

    location = "'{storage_path}'".format(storage_path = sps.conf.get('storage.datamart'))

    sps.sql(f"CREATE TABLE IF NOT EXISTS WTDATA ( \
                    TIMESTAMP TIMESTAMP, \
                    TURBINE_ID INT, \
                    WIND_SPEED DOUBLE, \
                    WIND_DIRECTION INT, \
                    POWER_OUTPUT DOUBLE, \
                    OUTPUTDATE DATE, \
                    IS_MISSING BOOLEAN, \
                    WIND_SPEED_CLEANED DOUBLE, \
                    WIND_DIRECTION_CLEANED INT, \
                    POWER_OUTPUT_CLEANED DOUBLE ) \
            USING DELTA \
            LOCATION {location}")
    
    sps.sql(f"MERGE INTO WTDATA \
        USING ( \
            SELECT \
                wtdata_validated.timestamp, \
                wtdata_validated.turbine_id, \
                wtdata_validated.wind_speed, \
                wtdata_validated.wind_direction, \
                wtdata_validated.power_output, \
                wtdata_validated.outputdate, \
                wtdata_validated.is_missing, \
                wtdata_validated.wind_speed_cleaned, \
                wtdata_validated.wind_direction_cleaned, \
                wtdata_validated.power_output_cleaned \
            FROM wtdata_validated \
        ) AS wtdata_validated \
        ON WTDATA.TIMESTAMP = wtdata_validated.timestamp \
            AND WTDATA.TURBINE_ID = wtdata_validated.turbine_id \
        WHEN NOT MATCHED THEN \
            INSERT ( \
                TIMESTAMP, \
                TURBINE_ID, \
                WIND_SPEED, \
                WIND_DIRECTION, \
                POWER_OUTPUT, \
                OUTPUTDATE, \
                IS_MISSING, \
                WIND_SPEED_CLEANED, \
                WIND_DIRECTION_CLEANED, \
                POWER_OUTPUT_CLEANED \
            ) VALUES ( \
                wtdata_validated.timestamp, \
                wtdata_validated.turbine_id, \
                wtdata_validated.wind_speed, \
                wtdata_validated.wind_direction, \
                wtdata_validated.power_output, \
                wtdata_validated.outputdate, \
                wtdata_validated.is_missing, \
                wtdata_validated.wind_speed_cleaned, \
                wtdata_validated.wind_direction_cleaned, \
                wtdata_validated.power_output_cleaned \
            )")


def get_latestvalidated_wtdata(sps: SparkSession, pqoptions) -> DataFrame:

    df_val_wtdata = read_parquet(sps, "{storage_path}".format(storage_path = sps.conf.get('storage.validated')), pqoptions)
    
    return df_val_wtdata