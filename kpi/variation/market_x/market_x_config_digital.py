from pyspark.sql.functions import udf, col, trim
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StringType

def market_x_config_digital(config_dict, data_dict):
    config_dict['pre_weeks'] = 8