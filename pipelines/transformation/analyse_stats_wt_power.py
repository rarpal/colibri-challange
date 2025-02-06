from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pipelines.util.platform import start_spark
from pipelines.util.storage import read_csv,read_csvfolder,write_partby_parquet,write_parquet,read_parquet_filter,read_parquet

def analyse_stats_wt_power(sps: SparkSession):

    location = "'{storage_path}/STATS_WT_POWER'".format(storage_path = sps.conf.get('storage.datamart'))

    sps.sql(f"CREATE OR REPLACE TABLE STATS_WT_POWER \
            USING DELTA \
            SELECT \
                TURBINE_ID, \
                MIN(POWER_OUTPUT_CLEANED) AS MIN_POWER, \
                MAX(POWER_OUTPUT_CLEANED) AS MAX_POWER, \
                AVG(POWER_OUTPUT_CLEANED) AS AVG_POWER \
            FROM WTDATA \
            GROUP BY \
                TURBINE_ID" \
            )