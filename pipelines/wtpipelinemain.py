# Wind Turbine Main Pipeline

"""
1. Start Spark
2. Call extraction for typed wt data :- store typed extract partitioned by date and turbine id
3. Call validattion for wt data :- store typed extract with validation tags
4. Call transformations :- store as delta lake tables
5. Stop Spark

"""

from pipelines.util.platform import start_spark
from pipelines.extraction import extract_wtdata
from pipelines.validation import validate_wtdata
from pipelines.transformation import analyse_anomalies_wt_power,analyse_stats_wt_power

sps, logger, conf = start_spark(app_name='wtdata_pipeline')

extract_wtdata.extract_wtdata(sps)

validate_wtdata.validate_wtdata(sps)

analyse_anomalies_wt_power.analyse_anomalies_wt_power(sps)

analyse_stats_wt_power.analyse_stats_wt_power(sps)

sps.stop()