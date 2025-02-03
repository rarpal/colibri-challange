# Introduction

This project builds a data pipeline to extract and ingest raw data from a set of wind turbines that generate power based on wind speed and direction, and their power output is measured in megawatts.

The pipeline does the following:
- Validates and cleans the data based on some predetermined dataset parameters
- Calculates summary statistics for each turbine based on the period covered by the provided data
- Identifies power output anomalies for each turbine over the covered period using their mean and standard deviation
- Stored the validated and cleansed data along with the analysis produced in a database which can be used for further analysis

# Tooling

The following main tools and resources have been used for the developent of this data pipeline:
- Python 3.10
- PySpark 3.5.3 in stand-alone local mode
- Spark configured to support Delta Lake
- Pytest unit testing framework
- Conda environment and package manager
- All package dependancies have been exported to environment.yml
- Hosted on Windows 11 Pro for Workstations

# Assumptions

Since the requirements for this project was gathered from a limited breif, some assumptions have been made based on profiling the provided dataset:
- For data validation and cleansing purposes, the range of wind speeds, wind directions and power outputs have been pre-determined based on the provided dataset
- For the days where data is missing in the ingested data files, the pipeline would fill those based on previous days data
- The summary statistics and anomaly analysis are based on the period available in the provied dataset
- The pipeline would extract and load incomming data on an incremental basis

# Limitations

This Data Pipeline project is for development and presentation only and is not production ready. However it contains the structure and framework to proceed further into a full production ready version based on a more refined set of requirements and time allocation. The following are some of the current limitations:
- Limited set of test cases used to demonstrate the capabilities of the testing framework
- Not optimaised for performance due to the limited compute and other infrastructure resources
- No CI/CD pipelines have been built

# Further work

What further work could be done

# Outputs

Any outputs and test results

