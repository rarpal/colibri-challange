from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pipelines.util.platform import start_spark
from pipelines.util.storage import read_csv,read_csvfolder,write_partby_parquet,write_parquet,read_parquet_filter
from pipelines.extraction.extract_wtdata import get_latestextract_wtdata
from pipelines.metadata import MetaData

def validate_wtdata(sps: SparkSession) -> DataFrame:
    
    md = MetaData()
    watermark_date = md.get_watermark_date(sps)
    sch = T.StructType([
        T.StructField("timestamp", T.TimestampType(), True),
        T.StructField("turbine_id", T.IntegerType(), True),
        T.StructField("wind_speed", T.DoubleType(), True),
        T.StructField("wind_direction", T.IntegerType(), True),
        T.StructField("power_output", T.DoubleType(), True),
        T.StructField("outputdate", T.DateType(), True)
    ])
    pqoptions = {
        "schema": sch,
        "inferSchema": "false"
    }

    wtdata_latest = get_latestextract_wtdata(sps, md, pqoptions)

    if (watermark_date != "1900-01-01"):
        wtdata_missing = validate_missing_days(sps, md, wtdata_latest, pqoptions)
        wtdata_validated = wtdata_missing.union(wtdata_latest)
    else:
        wtdata_validated = wtdata_latest.withColumn("is_missing", F.lit(False))
    
    wtdata_validated = validate_wind_speed(wtdata_validated)
    wtdata_validated = validate_power_output(wtdata_validated)
    wtdata_validated = validate_wind_direction(wtdata_validated)

    write_parquet(sps, wtdata_validated, "{storage_path}".format(storage_path = sps.conf.get('storage.validated')))

    md.set_watermark_date(sps, wtdata_validated)

    return wtdata_validated

def validate_wind_speed(wtdata: DataFrame) -> DataFrame:
    
    return wtdata.withColumn("wind_speed_cleaned", 
                      F.when(F.col("wind_speed") < MetaData.min_wind_speed, MetaData.min_wind_speed). \
                      when(F.col("wind_speed") > MetaData.max_wind_speed, MetaData.max_wind_speed). \
                      otherwise(F.col("wind_speed"))
                      )

def validate_wind_direction(wtdata: DataFrame) -> DataFrame:
    
    return wtdata.withColumn("wind_direction_cleaned", 
                      F.when(F.col("wind_direction") < MetaData.min_wind_direction, MetaData.min_wind_direction).
                      when(F.col("wind_direction") > MetaData.max_wind_direction, MetaData.max_wind_direction).
                      otherwise(F.col("wind_direction"))
                      )

def validate_power_output(wtdata: DataFrame) -> DataFrame:
    
    return wtdata.withColumn("power_output_cleaned", 
                      F.when(F.col("power_output") < MetaData.min_power_output, MetaData.min_power_output).
                      when(F.col("power_output") > MetaData.max_power_output, MetaData.max_power_output).
                      otherwise(F.col("power_output"))
                      )

def validate_missing_days(sps: SparkSession, md: MetaData, wtdata: DataFrame, pqoptions) -> DataFrame:

    df_last_wtdata = get_lastvalidated_wtdata(sps, md, pqoptions)

    df_last_maxts = df_last_wtdata.agg(F.max(F.col("timestamp")).alias("last_maxts"))
    df_latest_mints = wtdata.agg(F.min(F.col("timestamp")).alias("latest_mints"))
    df_dayslapsed = df_last_maxts.crossJoin(df_latest_mints).withColumn("dayslapsed",F.datediff(F.col("latest_mints"),F.col("last_maxts"))-1)
    
    df_last_wtdata = df_last_wtdata.crossJoin(df_dayslapsed). \
        drop("wind_speed_cleaned","wind_direction_cleaned","power_output_cleaned")

    df_missing_wtdata = df_last_wtdata.withColumn("seq", F.sequence(F.lit(0),F.col("dayslapsed"))).withColumn("seqex", F.explode("seq")). \
        withColumn("timestamp",F.to_timestamp(F.concat(F.date_add(F.col("timestamp"),F.col("seqex")),F.date_format(F.col("timestamp")," HH:mm:ss")),"yyyy-MM-dd HH:mm:ss")). \
        withColumn("outputdate",F.to_date(F.col("timestamp"))). \
        withColumn("is_missing", F.lit(True)). \
        filter(F.col("outputdate") > md.watermark_date). \
        drop("last_maxts","latest_mints","dayslapsed","seq","seqex")
    
    return df_missing_wtdata

def get_lastvalidated_wtdata(sps: SparkSession, md: MetaData, pqoptions) -> DataFrame:

    df_val_wtdata = read_parquet_filter(sps, F.col("outputdate") == md.watermark_date, "{storage_path}".format(storage_path = sps.conf.get('storage.validated')), pqoptions)

    return df_val_wtdata