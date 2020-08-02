# from pyspark import SparkContext
# from pyspark.sql import HiveContext

import pyspark.sql.functions as func
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit

from core_modules.utils import functions

import logging

# ch = logging.StreamHandler(sys.stdout)
# ch.setLevel(logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# ch.setFormatter(formatter)

# sc = SparkContext.getOrCreate()
# sqlContext = HiveContext(sc)

# logging.basicConfig(
#     format='%(asctime)s %(levelname)s:%(message)s', 
#     level=logging.DEBUG, 
#     datefmt='%I:%M:%S'
# )
logger = logging.getLogger()

class Singleton(type):
    """
    Meta class to create a singleton class
    """
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
        
def pre_processor(method, name, df):
#     if name in ['pre_period', 'post_period', 'dur_period', 'pre_period_basket', 'post_period_basket', 'dur_period_basket']:
#         df = df.filter(df.basket_sample_seed_num == '1')
    print 'Applying lambda on : ' + name + ' with schema ' + str(df)
    return method(df)
    
def read_app_file(path):
    # read data as rdd
    app_customer_rdd = sc.textFile(path)

    # schema creation for app data file
    app_schema = StructType(
        [StructField(identity_type_code, StringType(), True),
         StructField("first_login", StringType(), True),
         StructField("last_login", StringType(), True)]
    )
    
    # process rdd by splitting each recorder by ';'
    processed_rdd = app_customer_rdd.map(lambda line: line.split(";"))

    # apply schema
    app_customer_df = processed_rdd.toDF(app_schema)
    adjusted_app_customer_df = app_customer_df.withColumn(
        identity_type_code, concat(lit(0), col(identity_type_code))
    )
    return adjusted_app_customer_df

def read_resamp_file(path):
    # read data as rdd
    resamp_rdd = sc.textFile(path)
    
    # schema creation for app data file
    resamp_schema = StructType(
        [StructField(identity_type_code, StringType(), True),
         StructField("event_control_flag", StringType(), True)]
    )
    
    # process rdd by splitting each recorder by ','
    processed_rdd = resamp_rdd.map(lambda line: line.split(","))

    # apply schema
    resamp_df = processed_rdd.toDF(resamp_schema)
    return resamp_df

def read_file(sc, path, seperator, schema):
    rdd = sc.textFile(path)
    processed_rdd = rdd.map(lambda line: line.split(seperator))
    df = processed_rdd.toDF(schema)
    return df

def valid_df(df):
    if df == None:
        return False
#     elif len(df.take(1)) == 0:
#     elif df.rdd.isEmpty():
#         return False
    else:
        return True

 # perform aggreation at different levels
def sum(sqlContext, input_df, grouping_set, column_set, measure, col1):
    input_df = input_df.fillna('XX', column_set)
    
    columns = column_set + [measure] + ['grouping_level']
    
    df = sqlContext.createDataFrame(
        [[''] * len(columns)],
        columns
    ).filter(col(columns[1]) != '')
    
    for i in range(len(grouping_set)):    
        group = str(
            grouping_set[i]
        ).replace('(','').replace(')','').replace("'","").replace(" ","").split(',')
        
        grouping_lvl = '-'.join(group)

        if str(grouping_set[i]) == str(()):
            grouped_df = input_df.groupby().agg(
                (func.sum(col1)).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level', lit('overall')
            )
        else:
            grouped_df = input_df.groupby(*group).agg(
                (func.sum(col1)).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit(grouping_lvl)
            )
            
        df = functions.union_multi_df(
            df,
            grouped_df,
            column_sequence_df = 1
        ).fillna('total', column_set)
    return df

 # perform count at different levels
def count(sqlContext, input_df, grouping_set, column_set, measure, col1):
    
    input_df = input_df.fillna('XX', column_set)
    print 'input_df'
    print input_df.count()
    
    columns = column_set + [measure] + ['grouping_level']
    
    df = sqlContext.createDataFrame(
        [[''] * len(columns)],
        columns
    ).filter(col(columns[1]) != '')
    
    for i in range(len(grouping_set)):    
        group = str(
            grouping_set[i]
        ).replace('(','').replace(')','').replace("'","").replace(" ","").split(',')
        
        grouping_lvl = '-'.join(group)

        if str(grouping_set[i]) == str(()):
            grouped_df = input_df.groupby().agg(
                func.count(col1).alias(measure)
            ).fillna(0, measure)
            print 'grouped_df'
#             grouped_df.cache()
#             grouped_df.show()
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit('overall')
            )
        else:
            grouped_df = input_df.groupby(*group).agg(
                func.count(col1).alias(measure)
            ).fillna(0, measure)
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit(grouping_lvl)
            )
            
        df = functions.union_multi_df(
            df,
            grouped_df,
            column_sequence_df = 1
        ).fillna('total', column_set)
    return df

 # perform distinct count at different levels
def distinct_count(sqlContext, input_df, grouping_set, column_set, measure, col1):
    input_df = input_df.fillna('XX', column_set)
    columns = column_set + [measure] + ['grouping_level']
    
    df = sqlContext.createDataFrame(
        [[''] * len(columns)],
        columns
    ).filter(col(columns[1]) != '')
    
    for i in range(len(grouping_set)):    
        group = str(
            grouping_set[i]
        ).replace('(','').replace(')','').replace("'","").replace(" ","").split(',')
        
        grouping_lvl = '-'.join(group)

        if str(grouping_set[i]) == str(()):
            grouped_df = input_df.groupby().agg(
                func.countDistinct(col1).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit('overall')
            )
        else:
            grouped_df = input_df.groupby(*group).agg(
                func.countDistinct(col1).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit(grouping_lvl)
            )
            
        df = functions.union_multi_df(
            df,
            grouped_df,
            column_sequence_df = 1
        ).fillna('total', column_set)
    return df

 # extract average at different levels
def average(sqlContext, input_df, grouping_set, column_set, measure, col1, col2):
    input_df = input_df.fillna('XX', column_set)
    columns = column_set + [measure] + ['grouping_level']
    
    df = sqlContext.createDataFrame(
        [[''] * len(columns)], columns
    ).filter(col(columns[1]) != '')
    
    for i in range(len(grouping_set)):    
        group = str(
            grouping_set[i]
        ).replace('(','').replace(')','').replace("'","").replace(" ","").split(',')
        
        grouping_lvl = '-'.join(group)

        if str(grouping_set[i]) == str(()):
            grouped_df = input_df.groupby().agg(
                (func.sum(col1)/func.count(col2)).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit('overall')
            )
        else:
            grouped_df = input_df.groupby(*group).agg(
                (func.sum(col1)/func.count(col2)).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit(grouping_lvl)
            )
            
        df = functions.union_multi_df(
            df,
            grouped_df,
            column_sequence_df = 1
        ).fillna('total', column_set)
    return df

def distinct_average(sqlContext, input_df, grouping_set, column_set, measure, col1, col2):
    input_df = input_df.fillna('XX', column_set)
    columns = column_set + [measure] + ['grouping_level']
    
    df = sqlContext.createDataFrame(
        [[''] * len(columns)],
        columns
    ).filter(col(columns[1]) != '')
    
    for i in range(len(grouping_set)):    
        group = str(
            grouping_set[i]
        ).replace('(','').replace(')','').replace("'","").replace(" ","").split(',')
        
        grouping_lvl = '-'.join(group)

        if str(grouping_set[i]) == str(()):
            grouped_df = input_df.groupby().agg(
                (func.sum(col1)/func.countDistinct(col2)).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit('overall')
            )
        else:
            grouped_df = input_df.groupby(*group).agg(
                (func.sum(col1)/func.countDistinct(col2)).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit(grouping_lvl)
            )
            
        df = functions.union_multi_df(
            df,
            grouped_df,
            column_sequence_df = 1
        ).fillna('total', column_set)
    return df

 # perform average of distinct elements at different levels   
def distinct_count_average(sqlContext, input_df, grouping_set, column_set, measure, col1, total):
    input_df = input_df.fillna('XX', column_set)
    columns = column_set + [measure] + ['grouping_level']
    
    df = sqlContext.createDataFrame(
        [[''] * len(columns)],
        columns
    ).filter(col(columns[1]) != '')
    
    for i in range(len(grouping_set)):    
        group = str(
            grouping_set[i]
        ).replace('(','').replace(')','').replace("'","").replace(" ","").split(',')
        
        grouping_lvl = '-'.join(group)

        if str(grouping_set[i]) == str(()):
            grouped_df = input_df.groupby().agg(
                (func.countDistinct(col1)/total).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit('overall')
            )
        else:
            grouped_df = input_df.groupby(*group).agg(
                (func.countDistinct(col1)/total).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit(grouping_lvl)
            )
            
        df = functions.union_multi_df(
            df,
            grouped_df,
            column_sequence_df = 1
        ).fillna('total', column_set)
    return df

# extract average at different levels
def count_average(sqlContext, input_df, grouping_set, column_set, measure, col1, total):
    input_df = input_df.fillna('XX', column_set)
    columns = column_set + [measure] + ['grouping_level']
    
    df = sqlContext.createDataFrame(
        [[''] * len(columns)],
        columns
    ).filter(col(columns[1]) != '')
    
    for i in range(len(grouping_set)):    
        group = str(
            grouping_set[i]
        ).replace('(','').replace(')','').replace("'","").replace(" ","").split(',')
        
        grouping_lvl = '-'.join(group)

        if str(grouping_set[i]) == str(()):
            grouped_df = input_df.groupby().agg(
                (func.count(col1)/total).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit('overall')
            )
        else:
            grouped_df = input_df.groupby(*group).agg(
                (func.count(col1)/total).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit(grouping_lvl)
            )
            
        df = functions.union_multi_df(
            df,
            grouped_df,
            column_sequence_df = 1
        ).fillna('total', column_set)
    return df

def percentage(sqlContext, input_df, grouping_set, column_set, measure, col1, total):
    input_df = input_df.fillna('XX', column_set)
    columns = column_set + [measure] + ['grouping_level']
    
    df = sqlContext.createDataFrame(
        [[''] * len(columns)],
        columns
    ).filter(col(columns[1]) != '')
    
    for i in range(len(grouping_set)):    
        group = str(
            grouping_set[i]
        ).replace('(','').replace(')','').replace("'","").replace(" ","").split(',')
        
        grouping_lvl = '-'.join(group)

        if str(grouping_set[i]) == str(()):
            grouped_df = input_df.groupby().agg(
                (func.countDistinct(col1)*100/total).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit('overall')
            )
        else:
            grouped_df = input_df.groupby(*group).agg(
                (func.countDistinct(col1)*100/total).alias(measure)
            ).fillna(0, measure)
            
            grouped_df = grouped_df.withColumn(
                'grouping_level',
                lit(grouping_lvl)
            )
            
        df = functions.union_multi_df(
            df,
            grouped_df,
            column_sequence_df = 1
        ).fillna('total', column_set)
    return df
