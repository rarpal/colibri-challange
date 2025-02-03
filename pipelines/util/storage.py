import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame, types as T

def read_csv(sps: SparkSession, csvpath: str, csvoptions: dict) -> DataFrame:
    
    try:
    
        df_csv = sps.read.csv(csvpath, **csvoptions)

        return df_csv

    except Exception as e:

        return f"TODO: an empty data frame should be returned"


def read_csvfolder(sps: SparkSession, csvfolderpath: str, csvoptions: dict) -> DataFrame:

    try:
    
        file_list = []

        for filename in os.listdir(csvfolderpath):
            if filename.endswith(".csv"):
                file_path = os.path.join(csvfolderpath, filename)
                file_list.append(file_path)

        df_csv = sps.read.csv(file_list, **csvoptions)

        return df_csv
    
    except Exception as e:

        return f"TODO: an empty data frame should be returned"
    

def read_parquet(sps: SparkSession, parquetpath: str, pqoptions: dict) -> DataFrame:
    
    try:

        # df_parquet = sps.read.parquet(parquetpath)
        df_parquet = sps.read.format("parquet").options(**pqoptions).load(parquetpath)
        
        return df_parquet
    
    except Exception as e:

        return f"TODO: an empty data frame should be returned"
    

def read_parquet_filter(sps: SparkSession, filtercondition: str, parquetpath: str, pqoptions: dict) -> DataFrame:
    
    try:

        #df_parquet = sps.read.options(**pqoptions).parquet(parquetpath).filter(filtercondition)
        df_parquet = sps.read.format("parquet").options(**pqoptions).load(parquetpath).filter(filtercondition)

        return df_parquet
    
    except Exception as e:

        return f"TODO: an empty data frame should be returned"
    

def write_repart_parquet(sps: SparkSession, df_parquet: DataFrame, numparts: int, parquetpath: str):
    
    df_parquet.repartition(numparts).write.mode('overwrite').parquet(parquetpath)

def write_partby_parquet(sps: SparkSession, df_parquet: DataFrame, partcols: list, parquetpath: str):
    
    df_parquet.write.partitionBy(*partcols).mode('overwrite').parquet(parquetpath)

def write_parquet(sps: SparkSession, df_parquet: DataFrame, parquetpath: str):

    df_parquet.write.mode('overwrite').parquet(parquetpath)

def write_delta(sps: SparkSession, df_delta: DataFrame, deltapath: str):

    df_delta.write.format('delta').mode('overwrite').save(deltapath)

def write_deltatable(sps: SparkSession, df_delta: DataFrame, deltatable: str):

    df_delta.write.format('delta').mode('overwrite').saveAsTable(deltatable)
