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

This Data Pipeline project is for development and presentation only and is not production ready. However it contains the structure and framework to to scale and extend further into a full production ready version based on a more refined set of requirements and time allocation. The following are some of the current limitations:
- Limited set of test cases used to demonstrate the capabilities of the testing framework
- Not optimaised for performance due to the limited compute and other infrastructure resources
- No provisions for any CI/CD have been considered

# Further work

- Based on a more detailed set of requirements and business rules, additional validation and cleansing processors could be added to the pipeline
- The Metadata catalog can be further enhanced to accomodate both incremental and full refresh loads
- A data cleardown and archiving pipeline can be added to reduce and maintain the data volumne from overgrowing and impacting performance in the long run
- More comprehensive error handling and pipeline monitoring could be added for maintainance purposes

# Results Outputs

### Wind Turbine Power Output anomalies for the whole period can be analyised by querying the following table produced by the pipeline

SELECT * FROM ANOMALIES_WT_POWER
+----------+------------------+------------------+-------------------+----------------+------------------+----------+
|TURBINE_ID|TURBINE_MEAN      |ALL_MEAN          |ALL_STDDEV         |ALL_STDDEV_H2   |ALL_STDDEV_L2     |IS_ANOMALY|
+----------+------------------+------------------+-------------------+----------------+------------------+----------+
|1         |3.0163978494623653|3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|13        |3.0313172043010743|3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|6         |2.983870967741935 |3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|3         |2.979973118279571 |3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|5         |3.016532258064516 |3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|15        |3.0364247311827954|3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|9         |3.0028225806451614|3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|4         |2.946370967741935 |3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|8         |2.9848118279569897|3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|7         |3.0126344086021506|3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|10        |3.006048387096774 |3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|11        |2.9591397849462364|3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|14        |3.0177419354838713|3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|2         |2.9836021505376347|3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
|12        |3.051209677419354 |3.0019265232974908|0.02883681738983466|3.05960015807716|2.9442528885178216|0         |
+----------+------------------+------------------+-------------------+----------------+------------------+----------+


### Wind Turbine Summary Statistics for the whole period can be analyised by querying the following table produced by the pipeline

SELECT * FROM STATS_WT_POWER
+----------+---------+---------+------------------+
|TURBINE_ID|MIN_POWER|MAX_POWER|AVG_POWER         |
+----------+---------+---------+------------------+
|1         |1.5      |4.5      |3.0163978494623653|
|13        |1.5      |4.5      |3.0313172043010743|
|6         |1.5      |4.5      |2.983870967741935 |
|3         |1.5      |4.5      |2.979973118279571 |
|5         |1.5      |4.5      |3.016532258064516 |
|15        |1.5      |4.5      |3.0364247311827954|
|9         |1.5      |4.5      |3.0028225806451614|
|4         |1.5      |4.5      |2.946370967741935 |
|8         |1.5      |4.5      |2.9848118279569897|
|7         |1.5      |4.5      |3.0126344086021506|
|10        |1.5      |4.5      |3.006048387096774 |
|11        |1.5      |4.5      |2.9591397849462364|
|14        |1.5      |4.5      |3.0177419354838713|
|2         |1.5      |4.5      |2.9836021505376347|
|12        |1.5      |4.5      |3.051209677419354 |
+----------+---------+---------+------------------+


### Additional analysis can be conducted by querying the following Wind Turbine Data table loaded by the pipeline

DESCRIBE TABLE WTDATA
+----------------------+---------+-------+
|col_name              |data_type|comment|
+----------------------+---------+-------+
|TIMESTAMP             |timestamp|NULL   |
|TURBINE_ID            |int      |NULL   |
|WIND_SPEED            |double   |NULL   |
|WIND_DIRECTION        |int      |NULL   |
|POWER_OUTPUT          |double   |NULL   |
|OUTPUTDATE            |date     |NULL   |
|IS_MISSING            |boolean  |NULL   |
|WIND_SPEED_CLEANED    |double   |NULL   |
|WIND_DIRECTION_CLEANED|int      |NULL   |
|POWER_OUTPUT_CLEANED  |double   |NULL   |
+----------------------+---------+-------+


### Results of the limited set of Units Tests performed is shows below

> pytest
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