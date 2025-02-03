from pyspark.sql import functions as F
from pyspark.sql import types as T

class TestValidations:

    def test_validate_wind_speed(self, sps, testdata_validation, validate_wtdata, metadata):

        df = validate_wtdata.validate_wind_speed(testdata_validation)

        test_count = df.filter( (F.col("wind_speed_cleaned") < metadata.min_wind_speed) | ((F.col("wind_speed_cleaned") > metadata.max_wind_speed)) ).count()

        assert (test_count == 0), f"test_validate_wind_speed expects 0 but {test_count} was returned !"


    def test_validate_power_output(self, sps, testdata_validation, validate_wtdata, metadata):

        df = validate_wtdata.validate_power_output(testdata_validation)

        test_count = df.filter( (F.col("power_output_cleaned") < metadata.min_power_output) | ((F.col("power_output_cleaned") > metadata.max_power_output)) ).count()

        assert (test_count == 0), f"test_validate_power_output expects 0 but {test_count} was returned !"


    def test_validate_wind_direction(self, sps, testdata_validation, validate_wtdata, metadata):

        df = validate_wtdata.validate_wind_direction(testdata_validation)

        test_count = df.filter( (F.col("wind_direction_cleaned") < metadata.min_wind_direction) | ((F.col("wind_direction_cleaned") > metadata.max_wind_direction)) ).count()

        assert (test_count == 0), f"test_validate_wind_direction expects 0 but {test_count} was returned !"

