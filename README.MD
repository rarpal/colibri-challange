# Introduction

This project builds a data pipeline to extract and ingest raw data from a set of wind turbines that generate power based on wind speed and direction with their power outputs measured in megawatts.

The pipeline does the following:
- Validates and cleans the data based on some predetermined dataset parameters
- Calculates summary statistics for each turbine based on the period covered by the provided data
- Identifies power output anomalies for each turbine over the covered period using their mean and standard deviation
- Stores the validated and cleansed data along with the analysis produced in a database which can be used for further analysis

# Tooling

The following main tools and resources have been used for the developent of this data pipeline:
- Python 3.10
- PySpark 3.5.3 in stand-alone local mode
- Spark configured to support Delta Lake
- Pytest unit testing framework
- Conda environment and package manager
- All package dependancies have been exported to environment.yml
- Hosted on Windows 11 Pro for Workstations

# Installing the Pipeline

- Clone the Github Repo at https://github.com/rarpal/colibri-challange.git
- Create a python environment with Conda using the provided environment.yml
    - or use alternative Environment and Package Manager using the same dependancies
- Install the dependancy packages (Conda also does this as part of environment creation from the .yml file)
- Setup Spark and Hadoop environment variables id not already done

Note:- This Pipeline has been tested on a local development environment based on the tooling and resources mentioned above. Other environments may require some minor configuration changes to work correctly. 

# Running the Pipeline

The main driver pipeline is located in the /pipelines folder and named wtpipelinemain.py. Once the installation and environment configuration is in place, the pipeline can be executed locally by simply opening a terminal and issuing the following commands:

    cd /pipelines
    python wtpipelinemain

The pipeline will use the dataset located in the /data/landed folder.

This pipeline could also be run on a cluster environment like Databricks by creating a package and uploading to a cluster and then either setting up a workflow job or issuing the spark-submit command. However this has not been tested on such an environment so far.

# Assumptions

Since the requirements for this project was gathered from a limited breif, some assumptions have been made based on profiling the provided dataset:
- For data validation and cleansing purposes, A valid range of wind speeds, wind directions and power outputs have been pre-determined from the provided dataset
- For the days where data is missing in the ingested data files, the pipeline would fill those based on previous days data
- The summary statistics and anomaly analysis are based on the full period available in the provied dataset
- The pipeline would extract and load incomming data on an incremental basis

# Limitations

This data pipeline is for development and presentation only and is not yet production ready. However it contains the structure and framework to be able to scale and extend further into a full production ready version given a more refined set of requirements and sufficient time. The following are some of the current limitations:
- Limited set of test cases used to demonstrate the capabilities of the testing framework
- Not optimaised for performance on larger datasets and higher ingestion volumes
- No provisions made for CI/CD

# Further work

- Based on a more detailed set of requirements and business rules, additional validation and cleansing processors could be added to the pipeline
- The Metadata catalog can be further enhanced to accomodate both incremental and full refresh loads
- A data cleardown and archiving pipeline can be added to reduce and maintain the data volumne from overgrowing and impacting performance in the long run
- More comprehensive error handling and pipeline monitoring could be added for maintainance purposes
- Deploy and test on a distrubuted cluster environment such as Spark or Databricks


# Results and Outputs

### Wind Turbine Power Output anomalies for the whole period can be analyised by querying the following table produced by the pipeline

![ANOMALIES_WT_POWER table](/images/anomalies_wt_power.png)


### Wind Turbine Summary Statistics for the whole period can be analyised by querying the following table produced by the pipeline

![STATS_WT_POWER](/images/stats_wt_power.png)


### Additional analysis can be conducted by querying the following Wind Turbine Data table loaded by the pipeline

![WTDATA](/images/wtdata.png)


### Results of the limited set of Units Tests performed is shows below

pytest

==================================================================== test session starts ===================================================================== 
platform win32 -- Python 3.10.16, pytest-7.4.4, pluggy-1.5.0
rootdir: C:\PalProjects\colibri-challange\code
plugins: anyio-4.6.2
collected 3 items                                                                                                                                              

tests\unit\test_validation.py ...                                                                                                                       [100%]

===================================================================== 3 passed in 11.50s =====================================================================
(C:\PalProjects\colibri-challange\code\.env) PS C:\PalProjects\colibri-challange\code> SUCCESS: The process with PID 31856 (child process of PID 1252) has been terminated.
SUCCESS: The process with PID 1252 (child process of PID 25432) has been terminated.
SUCCESS: The process with PID 25432 (child process of PID 6940) has been terminated.