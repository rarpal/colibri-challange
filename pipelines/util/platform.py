"""
Module containing helper function for use with Apache Spark
"""

import __main__

from os import environ, listdir, path
import json
from pyspark import SparkFiles, SparkConf
from pyspark.sql import SparkSession
import pyspark
from delta import *

from pipelines.util import logging


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}, clstmode='0'):
    """Start Spark session, get Spark logger and load config files.

    Determines cluster environment based on the existance of the environment variable CLUSTER_DB_HOME.
    If not present defaults to local mode
    """

    # detect execution environment
    #flag_repl = not(hasattr(__main__, '__file__'))
    flag_repl = hasattr(__main__, '__file__')
    flag_debug = 'DEBUG' in environ.keys()
    flag_dbr = 'CLUSTER_DB_HOME' in environ.keys()

    #print(clstmode)

    if ((not (flag_repl or flag_debug)) and (clstmode=='1' or flag_dbr)):
        # get Spark session factory
        print('cluster')
        spark_builder = (
            SparkSession
            .builder
            .config('storage.landed','abfss://landed@<storageaccount>.dfs.core.windows.net/<folder>')           # Replace storage account and folder
            .config('storage.extracted','abfss://extracted@<storageaccount>.dfs.core.windows.net/<folder>')     # Replace storage account and folder
            .appName(app_name + '_cluster'))
        
        # create spark session
        spark_sess = spark_builder.getOrCreate()
    else:
        # get Spark session factory
        print('local')
        spark_builder = (
            SparkSession
            .builder
            .master(master)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir",r"C:/PalProjects/colibri-challange/code/data/datamart/warehouse")
            .config('storage.landed',r'C:/PalProjects/colibri-challange/code/data/landed')
            .config('storage.extracted',r'C:/PalProjects/colibri-challange/code/data/extracted')
            .config('storage.validated',r'C:/PalProjects/colibri-challange/code/data/validated')
            .config('storage.datamart',r'C:/PalProjects/colibri-challange/code/data/datamart')
            .config('storage.metadata',r'C:/PalProjects/colibri-challange/code/data/metadata')
            .config('storage.testdata',r'C:/PalProjects/colibri-challange/code/tests/fixtures')
            .appName(app_name + '_local'))
        
        # Remember to create the $SPARK_HOME/conf/hive-site.xml to configure the location of the metastore_db folder as follows
        """
        <property>
            <name>javax.jdo.option.ConnectionURL</name>
            <value>jdbc:derby:;databaseName=C:/PalProjects/colibri-challange/code/data/datamart/metastore_db;create=true</value>
        </property>
        """

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)
        spark_builder.enableHiveSupport()

        # add other config params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

        # create spark session
        #spark_sess = spark_builder.getOrCreate()
        spark_sess = configure_spark_with_delta_pip(spark_builder).getOrCreate()

    # retrieve Spark logger object
    spark_logger = logging.Log4j(spark_sess)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_sess, spark_logger, config_dict

def start_spark_local(app_name='my_spark_app', master='local[*]'):

    # detect execution environment
    flag_repl = not(hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    spark_builder = (
            SparkSession
            .builder
            .master(master)
            .appName(app_name))
    
    spark_sess = spark_builder.getOrCreate()

    return spark_sess
