from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pipelines.util.platform import start_spark
from pipelines.util.storage import read_csv,read_csvfolder,write_partby_parquet,write_parquet,read_parquet_filter,read_parquet

def analyse_anomalies_wt_power(sps: SparkSession):

    location = "'{storage_path}/ANOMALIES_WT_POWER'".format(storage_path = sps.conf.get('storage.datamart'))

    sps.sql(f"CREATE OR REPLACE TABLE ANOMALIES_WT_POWER \
            USING DELTA \
            LOCATION {location} \
            WITH CTE1 AS ( \
                SELECT \
                    TURBINE_ID, \
                    TURBINE_MEAN, \
                    AVG(TURBINE_MEAN) OVER() AS ALL_MEAN, \
                    STDDEV(TURBINE_MEAN) OVER() AS ALL_STDDEV, \
                    ALL_MEAN + (ALL_STDDEV*2) AS ALL_STDDEV_H2, \
                    ALL_MEAN - (ALL_STDDEV*2) AS ALL_STDDEV_L2, \
                    CASE WHEN TURBINE_MEAN > ALL_STDDEV_H2 OR TURBINE_MEAN < ALL_STDDEV_L2 \
                        THEN 1 ELSE 0 END AS IS_ANOMALY \
                FROM ( \
                    SELECT \
                        TURBINE_ID, \
                        AVG(POWER_OUTPUT_CLEANED) AS TURBINE_MEAN \
                    FROM \
                        WTDATA \
                    GROUP BY \
                        TURBINE_ID \
                ) AS TURBINE_MEANS \
            ) \
            SELECT * FROM CTE1" \
            )