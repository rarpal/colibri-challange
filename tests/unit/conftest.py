import os
import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pipelines.util.platform import start_spark
from pipelines.util.storage import read_csv,read_csvfolder,write_partby_parquet,write_parquet,read_parquet_filter
from pipelines.metadata import MetaData
import pipelines.validation.validate_wtdata as module_validate_wtdata

@pytest.fixture(scope="session")
def sps():

    sps, logger, conf = start_spark(app_name='colibri-challange_testing')

    return sps

@pytest.fixture(scope="session")
def testdata_validation(sps):

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
    df_testdata = read_csv(sps, "{storage_path}/testdata_validation.csv".format(storage_path = sps.conf.get('storage.testdata')), csvoptions )

    return df_testdata

@pytest.fixture(scope="session")
def validate_wtdata():

    return module_validate_wtdata

@pytest.fixture(scope="session")
def metadata():

    md = MetaData()

    return md
