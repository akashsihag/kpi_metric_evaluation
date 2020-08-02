from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import col

sc = SparkContext()
sqlContext = HiveContext(sc)

sc.setLogLevel('OFF')

# Add environment dependencies here

import sys
import market_x

# core modules
from core_modules.utils import functions
from core_modules import summaries

import kpi
import time
from kpi.helper import Helper

import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s', 
    level=logging.DEBUG, 
    datefmt='%I:%M:%S'
)

logger = logging.getLogger()

# ch = logging.StreamHandler(sys.stdout)
# ch.setLevel(logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# ch.setFormatter(formatter)

print 'Start time : -> ' + time.asctime(time.localtime(time.time()))

client = market_x.client.Client('dev', sc, sqlContext)
config = functions.get_config("kpi/configuration/market_y/config_traditional.ini")

# create object of helper class 
obj = Helper(sc, config, client)

# run_module will trigger the kpis definitions and materialize the created dataframes on output_db
obj.run_modules()

# final_table will return the final dataframe and also save it to output_db 
final_df = obj.final_table()

final_df.cache()
final_df.show()

print 'End time : -> ' + time.asctime(time.localtime(time.time()))