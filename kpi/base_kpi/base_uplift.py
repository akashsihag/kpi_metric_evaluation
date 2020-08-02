import matplotlib
matplotlib.use('PDF')
from kpi.utilities import utils
from kpi.base_kpi.base_module import BaseModule
from kpi.base_kpi.base_cost import BaseCost

from core_modules import resampling
from core_modules import uplift_calculation
import pyspark.sql.functions as func
from pyspark.sql.types import DoubleType, StringType, IntegerType

import pandas as pd
from pyspark.sql import functions as F
import numpy as np
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.mllib.feature import StandardScaler
from pyspark.sql.functions import lit,concat
from pyspark.sql import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import udf

from pyspark.sql.functions import broadcast, col
from core_modules import resampling

from core_modules import uplift_calculation
import pyspark.sql.functions as func

class BaseUplift(BaseModule):
    """
    uplift module
    """
    def __init__(self, sc, client):
        super(BaseUplift, self).__init__(sc, client)
        
        self.group = {}
        self.filtered_df = {}
        self.data = []
        
        self.detail_seg_df = self.df_dict(self.config_dict['data_variation']).filter(col('contact_stage_code') == 'ALC')
        print 'self.detail_seg_df'
        self.detail_seg_df.cache()
        self.detail_seg_df.show()
        
        prod_df = self.df_dict('table3').select('prod_code')
        
        self.pre_df = self.df_dict('pre_period')
#         .join(prod_df, 'prod_code')
        self.post_df = self.df_dict('dur_period')
#         .join(prod_df, 'prod_code')
        
        if (self.config_dict['uplift_level'] == 'basket'):
#             pre_trans_df = self.pre_df.select('transaction_fid').distinct()
#             post_trans_df = self.post_df.select('transaction_fid').distinct()
            
            
            self.pre_df = self.df_dict('pre_period_basket', self.client)
#             .join(pre_trans_df, 'transaction_fid', 'inner')
            self.post_df = self.df_dict('dur_period_basket', self.client)
#     .join(post_trans_df, 'transaction_fid', 'inner')
        
        if (self.config_dict['external_resampling'] == 'Y'):
            resamp_schema = StructType(
                [
                    StructField(self.config_dict['identity_type_code'], StringType(), True),
                    StructField("event_control_flag", StringType(), True)
                ]
            )
#             resampled_df = utils.read_file(sc, self.config_dict['resamp_file'], ",", resamp_schema)
            resampled_df = self.sqlContext.read.parquet(self.config_dict['resamp_file'])
#             resampled_df = sqlContext.read.parquet('testing/sharanga/LCM_Evaluation/rs_input2_{}.parquet'.format('2017_wk49'))
            print 'resampled_df'
            print resampled_df.count()
            resampled_df.cache()
            resampled_df.show()
        
            resampled_df = resampled_df.withColumnRenamed('card_code', self.config_dict['identity_type_code'])
#             toggle_udf = udf(lambda col1: '0' if (col1 == '1') else ('0' if (col1 == '0') else '0'))
#             resampled_df = resampled_df.withColumn('test_panel', (toggle_udf(col('test_panel'))))
            self.resampled_df = resampled_df.select(self.config_dict['identity_type_code'], col('test_panel').alias('event_control_flag'))
#             resampled_df = resampled_df.join(self.detail_seg_df, [self.config_dict['identity_type_code']], 'inner')
            print 'resampled_df renamed'
            print self.resampled_df.count()
            self.resampled_df.cache()
            self.resampled_df.show()
            
        elif (self.config_dict['external_resampling'] == 'N'):
            pivoted_df = self.pre_df.groupBy(self.config_dict['identity_type_code']).pivot("fis_week_id").sum("spend").na.fill(0)
            column_list = pivoted_df.columns.pop(0)
            input_df = pivoted_df.join(self.detail_seg_df, self.config_dict['identity_type_code'], 'inner')
            input_cluster_df = input_df.fillna({'event_control_flag':'Y'})
            input_cluster_df.write.parquet('input_cluster_df.parquet', mode='overwrite')
            
            if (self.config_dict['resamp_type'] == "one_to_one_match"):
                resampled_df = resampling.one_to_one_match(self.client, input_data = 'input_cluster_df.parquet', key = self.config_dict['identity_type_code'], feature_vars = column_list, rand_gen = 'Y', strata_vars = [], TC_flag = 'event_control_flag', T_flag = 'N', C_flag = 'Y', Cluster_number = 100, Cluster_iterations = 100, Max_clus = 250)
            else:
                if (self.config_dict['clustering'] == 'Y'):
                    cluster_df = resampling.cluster(self.client, input_cluster_df, key = self.config_dict['identity_type_code'], feature_vars = column_list, clusters = 100, max_iterations = 100)
                else:
                    cluster_df = input_cluster_df.withColumn('cluster', lit(1))
                for column in column_list:
                    cluster_df = cluster_df.withColumn(column, cluster_df[column].cast(IntegerType()))
                cluster_df = cluster_df.fillna(0)
                cluster_df.write.parquet('df_cluster_1.parquet', mode='overwrite')
                
                resampled_df = resampling.sys_resamp(
                        self.client, cluster_df, 
                        key = self.config_dict['identity_type_code'], 
                        feature_vars = column_list, strata_vars = ['cluster'], 
                        TC_flag = 'event_control_flag',
                        T_flag = 'N',
                        C_flag = 'Y',
                        mat = 0.01,
                        thresh = 10
                    )
            resampled_df = sqlContext.createDataFrame(resampled_df)
            self.resampled_df = resampled_df.withColumn('test_panel', (volume_udf(col('event_control_flag'))))
        
    def trigger_kpi(self, grouping_set, measures):
        """
        triggers given measures with grouping sets and joins the
        result based on column_set.
        """
        column_set = list(
            set(str(grouping_set)
                .replace('(','')
                .replace(')','')
                .replace("'","")
                .replace("[","")
                .replace("]","")
                .replace(" ","")
                .split(','))
        )
        
        column_set = filter(None, column_set)
        joining_columns = column_set + ['grouping_level']
#         compute_dataframes = getattr(self, 'compute_dataframes')
        self.compute_dataframes(column_set, grouping_set)        

        self.module_df = self.sqlContext.createDataFrame([[''] * len(joining_columns)], joining_columns).filter(col(joining_columns[0]) != '')
        for i in range(len(measures)):
            method = getattr(self, measures[i])
            kpi_df = method(grouping_set, joining_columns, column_set, measures[i])
            if (kpi_df is not None):
                if(i == 0):
                    self.module_df = kpi_df
                else:
                    self.module_df = self.module_df.join(kpi_df, joining_columns, 'outer')
        return self.module_df
    
    def compute_dataframes(self, column_set, grouping_set):
#         lis = [self.config_dict['identity_type_code'], 'event_control_flag'] + column_set
#         self.detail_seg_df = self.detail_seg_df.select(lis).distinct()
#         self.prsn_df = self.detail_seg_df.join(
#             self.resampled_df.select(self.config_dict['identity_type_code'], 'event_control_flag'),
#             self.config_dict['identity_type_code']
#         ).select(
#             self.detail_seg_df["*"],
#             self.resampled_df.event_control_flag
#         ).drop(self.detail_seg_df.event_control_flag)
        
        self.prsn_df = self.resampled_df
        
        print 'self.prsn_df'
        print self.prsn_df.count()
        print self.prsn_df.count()
        print self.prsn_df.select('prsn_code').distinct().count()
        self.prsn_df.cache()
        self.prsn_df.show()
        if (self.config_dict['uplift_level'] == 'item'):
            self.pre_df = self.pre_df.groupby(self.config_dict['identity_type_code']).agg(
                F.count('prod_code').alias('pre_units'),
                F.sum('spend').alias('pre_spend'),
                F.countDistinct('transaction_fid').alias('pre_visits')
            )

            print 'self.pre_df'
            print self.pre_df.count()
            self.pre_df.cache()
            self.pre_df.show()

            self.post_df = self.post_df.groupby(self.config_dict['identity_type_code']).agg(
                F.count('prod_code').alias('post_units'),
                F.sum('spend').alias('post_spend'),
                F.countDistinct('transaction_fid').alias('post_visits')
            )

            print 'self.post_df'
            print self.post_df.count()
            self.post_df.cache()
            self.post_df.show()
        else:
            self.pre_df = self.pre_df.groupby(self.config_dict['identity_type_code']).agg(
                F.sum('basket_item_qty').alias('pre_units'),
                F.sum('spend').alias('pre_spend'),
                F.countDistinct('transaction_fid').alias('pre_visits')
            )

            print 'self.pre_df'
            print self.pre_df.count()
            self.pre_df.cache()
            self.pre_df.show()

            self.post_df = self.post_df.groupby(self.config_dict['identity_type_code']).agg(
                F.sum('basket_item_qty').alias('post_units'),
                F.sum('spend').alias('post_spend'),
                F.countDistinct('transaction_fid').alias('post_visits')
            )

            print 'self.post_df'
            print self.post_df.count()
            self.post_df.cache()
            self.post_df.show()
            
        
#         pre_post_df = self.pre_df.join(self.post_df, self.config_dict['identity_type_code'])
        
#         print 'pre_post_df'
#         print pre_post_df.count()
#         pre_post_df.cache()
#         pre_post_df.show()
        
        uplift_df = self.prsn_df.join(self.pre_df, self.config_dict['identity_type_code'], 'left_outer') 
        uplift_df = uplift_df.join(self.post_df, self.config_dict['identity_type_code'], 'left_outer')
        
        uplift_df = uplift_df.fillna(0, uplift_df.columns)
        
        print 'uplift_df'
        uplift_df.cache()
        print uplift_df.count()
        uplift_df.show()
        
#         volume_udf = udf(lambda col1: '1' if (col1 == 'Y') else '0')
#         uplift_df = uplift_df.withColumn('test_panel', (volume_udf(col('event_control_flag'))))
#         uplift_df = uplift_df.drop('event_control_flag')
#         uplift_df = uplift_df.withColumnRenamed('test_panel','event_control_flag')        
        uplift_df = uplift_df.drop('test_panel')
        
        self.uplift_df = uplift_df.withColumn('event_control_flag', uplift_df['event_control_flag'].cast(IntegerType())) 
        print 'self.uplift_df'
        self.uplift_df.cache()
        self.uplift_df.show()
        
        if (self.config_dict['reporting_level'] != 'overall'):
            counter = 0
            for group in grouping_set:
                group_distinct_values = self.uplift_df.select(col(group)).distinct().collect()
                for i in range(0, len(group_distinct_values)):
                    if (getattr(group_distinct_values[i], group)) is None:
                        pass
                    else:
                        self.data.append(getattr(group_distinct_values[i], group))
                        df_name = self.uplift_df.filter(col(group) == getattr(group_distinct_values[i], group))
                        self.filtered_df[self.data[counter]] = group
                        print group_distinct_values[i]
                        df_name.write.saveAsTable(self.config_dict['analysis_schema'] +'.' + self.data[counter] + '_group_df', mode = 'overwrite')
                        counter = counter+1
                                            
    def units_uplift(self, grouping_set, joining_columns, column_set, measure):
        if (self.config_dict['reporting_level'] == 'overall'):
            df = self.uplift_df
            if self.config_dict['uplift_method'] == "ancova":
                units_df = uplift_calculation.ancova(self.client, metric='units', input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_period_var = 'pre_units', post_period_var = 'post_units', test_control_flg = 'event_control_flag', confidence_Level_signi = 0.95, confidence_Level_directional = 0.8, n_bin = 1)
                units_uplift = units_df
            else:
                units_df = uplift_calculation.ratio(self.client, metric='units', input_sparkdf = df, identifier = self.config_dict['identity_type_code'], pre_vars = 'pre_units', post_vars = 'post_units', test_control_flg = 'event_control_flag' , confidence_Level_signi = 0.95, confidence_Level_directional = 0.8)
                units_uplift = units_df
        else:
            units_uplift = pd.DataFrame()
            for key in self.data:
                df = self.sqlContext.table(self.config_dict['analysis_schema'] +'.'+ key + '_group_df')
                if self.config_dict['uplift_method'] == "ancova":
                    units_df = uplift_calculation.ancova(self.client, metric='units', input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_period_var = 'pre_units', post_period_var = 'post_units', test_control_flg = 'event_control_flag', confidence_Level_signi = 0.95, confidence_Level_directional = 0.8, n_bin = 1)
                    units_df[self.filtered_df[key]] = key
                    units_df['grouping_level'] = self.filtered_df[key]
                else:
                    units_df = uplift_calculation.ratio(self.client, metric='units', input_sparkdf = df, identifier = self.config_dict['identity_type_code'], pre_vars = 'pre_units', post_vars = 'post_units', test_control_flg = 'event_control_flag' , confidence_Level_signi = 0.95, confidence_Level_directional = 0.8)
                    units_df[self.filtered_df[key]] = key
                    units_df['grouping_level'] = self.filtered_df[key]
                units_uplift = pd.concat([units_df, units_uplift], ignore_index=True)
        final_df = self.sqlContext.createDataFrame(units_uplift) 
        final_df = final_df.withColumnRenamed('Total_uplift', 'units_uplift')
        final_df.cache()
        final_df.show()
        if (self.config_dict['reporting_level'] == 'overall'):
            final = final_df.select('units_uplift')
            final = final.withColumn('grouping_level', F.lit('overall'))
        else:
            col_list = joining_columns + ['units_uplift']
            final = final_df.select(*col_list)
            print 'final_df 2'
        final.cache()
        final.show()
        return final

    def sales_uplift(self, grouping_set,joining_columns, column_set, measure): 
        if (self.config_dict['reporting_level'] == 'overall'):
            df = self.uplift_df
            df = df.withColumn("pre_spend", df["pre_spend"].cast(DoubleType())) 
            df = df.withColumn("post_spend", df["post_spend"].cast(DoubleType()))
            if self.config_dict['uplift_method'] == "ancova":
                sales_df = uplift_calculation.ancova(self.client, metric='sales', input_sparkdf = df, identifier = self.config_dict['identity_type_code'], pre_period_var = 'pre_spend', post_period_var = 'post_spend', test_control_flg ='event_control_flag', confidence_Level_signi = 0.95, confidence_Level_directional = 0.8, n_bin = 1)
                sales_uplift = sales_df
            else:
                sales_df = uplift_calculation.ratio(self.client,metric='sales',input_sparkdf = df, identifier = self.config_dict['identity_type_code'], pre_vars = 'pre_spend',post_vars = 'post_spend', test_control_flg ='event_control_flag' , confidence_Level_signi = 0.95,confidence_Level_directional = 0.8)
        else:
            sales_uplift = pd.DataFrame()
            for key in self.data:
                df = self.sqlContext.table(self.config_dict['analysis_schema'] +'.' + key + '_group_df').drop('slot_segment')
                df = df.withColumn("pre_spend", df["pre_spend"].cast(DoubleType())) 
                df = df.withColumn("post_spend", df["post_spend"].cast(DoubleType()))
                if self.config_dict['uplift_method'] == "ancova":
                    sales_df = uplift_calculation.ancova(self.client, metric='sales', input_sparkdf = df, identifier = self.config_dict['identity_type_code'], pre_period_var = 'pre_spend', post_period_var = 'post_spend', test_control_flg ='event_control_flag', confidence_Level_signi = 0.95, confidence_Level_directional = 0.8, n_bin = 1)
                    sales_df[self.filtered_df[key]] = key
                    sales_df['grouping_level'] = self.filtered_df[key]
                else:
                    sales_df = uplift_calculation.ratio(self.client,metric='sales',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_vars = 'pre_spend',post_vars = 'post_spend', test_control_flg ='event_control_flag' , confidence_Level_signi = 0.95,confidence_Level_directional = 0.8)
                    sales_df[self.filtered_df[key]] = key
                    print sales_df
                    sales_df['grouping_level'] = self.filtered_df[key]
                sales_uplift = pd.concat([sales_df, sales_uplift], ignore_index=True)
                print sales_uplift
        sales_uplift['Metric'] = sales_uplift['Metric'].astype('str')
        sales_uplift = sales_uplift.replace(np.nan, 'total', regex=True)
        final_df = self.sqlContext.createDataFrame(sales_uplift)  
        final_df = final_df.withColumnRenamed('Total_uplift', 'sales_uplift')
        print 'final_df 1'
        final_df.cache()
        final_df.show()
        if (self.config_dict['reporting_level'] == 'overall'):
            final = final_df.select('sales_uplift')
            final = final.withColumn('grouping_level', F.lit('overall'))
        else:
            col_list = joining_columns + ['sales_uplift']
            final = final_df.select(*col_list)
            print 'final_df 2'
        final_df.cache()
        final.show()
        return final
                       
    def visits_uplift(self, grouping_set,joining_columns, column_set, measure):
        if (self.config_dict['reporting_level'] == 'overall'):
            df = self.uplift_df
            if self.config_dict['uplift_method'] == "ancova":
                visits_df = uplift_calculation.ancova(self.client, metric='visits', input_sparkdf = df, identifier = self.config_dict['identity_type_code'], pre_period_var = 'pre_visits', post_period_var = 'post_visits' ,test_control_flg = 'event_control_flag', confidence_Level_signi = 0.95, confidence_Level_directional = 0.8, n_bin = 1)
                visits_uplift = visits_df
            else:
                visits_df = uplift_calculation.ratio(self.client,metric='visits',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_vars = 'pre_visits',post_vars = 'post_visits' ,test_control_flg = 'event_control_flag' , confidence_Level_signi = 0.95, confidence_Level_directional = 0.8)
                visits_uplift = visits_df
        else:
            visits_uplift = pd.DataFrame()
            for key in self.data:
                df = self.sqlContext.table(self.config_dict['analysis_schema'] +'.' + key + '_group_df')
                if self.config_dict['uplift_method'] == "ancova":
                    visits_df = uplift_calculation.ancova(self.client, metric='visits', input_sparkdf = df, identifier = self.config_dict['identity_type_code'], pre_period_var = 'pre_visits', post_period_var = 'post_visits' ,test_control_flg = 'event_control_flag', confidence_Level_signi = 0.95, confidence_Level_directional = 0.8, n_bin = 1)
                    visits_df[self.filtered_df[key]] = key
                    visits_df['grouping_level'] = self.filtered_df[key]
                else:
                    visits_df = uplift_calculation.ratio(self.client,metric='visits',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_vars = 'pre_visits',post_vars = 'post_visits' ,test_control_flg = 'event_control_flag' , confidence_Level_signi = 0.95, confidence_Level_directional = 0.8)
                    visits_df[self.filtered_df[key]] = key
                    visits_df['grouping_level'] = self.filtered_df[key]
                visits_uplift = pd.concat([visits_df,visits_uplift], ignore_index=True)
        visits_uplift['Metric'] = visits_uplift['Metric'].astype('str')
        visits_uplift = visits_uplift.replace(np.nan, 'total', regex=True)
        final_df = self.sqlContext.createDataFrame(visits_uplift)  
        final_df = final_df.withColumnRenamed('Total_uplift','visits_uplift')
        if (self.config_dict['reporting_level'] == 'overall'):
            final = final_df.select('visits_uplift')
            final = final.withColumn('grouping_level', F.lit('overall'))
        else:
            col_list = joining_columns + ['visits_uplift']
            final = final_df.select(*col_list)
            print 'final_df 2'
        final_df.cache()
        return final

    def adjusted_sales_uplift(self, grouping_set,joining_columns, column_set, measure):
        sales_uplift = pd.DataFrame()
#         filtered_df = {}
        for key in self.data:
            df = self.sqlContext.table(self.config_dict['analysis_schema'] +'.' + key + '_group_df')
            if self.config_dict['uplift_method'] == "ancova":
                sales_df = uplift_calculation.ancova(self.client, metric='sales', input_sparkdf = df, identifier = self.config_dict['identity_type_code'], pre_period_var = 'pre_spend', post_period_var = 'post_spend', test_control_flg = 'event_control_flag', confidence_Level_signi =0.95, confidence_Level_directional = 0.8, n_bin = 1)
                sales_df[self.filtered_df[key]] = key
                sales_df['grouping_level'] = self.filtered_df[key]
            else:
                sales_df = uplift_calculation.ratio(self.client,metric='sales',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_vars = 'pre_spend',post_vars = 'post_spend' ,test_control_flg ='event_control_flag' ,confidence_Level_signi =0.95,confidence_Level_directional = 0.8)
                sales_df[self.filtered_df[key]] = key
                print sales_df
                sales_df['grouping_level'] = self.filtered_df[key]
#             print type(sales_df)
#             sales_uplift = sales_df.append(sales_uplift, ignore_index=True)
            sales_uplift = pd.concat([sales_df,sales_uplift], ignore_index=True)
            print sales_uplift
            print sales_uplift.dtypes
        sales_uplift['Metric'] = sales_uplift['Metric'].astype('str')
        print sales_uplift.dtypes
        sales_uplift = sales_uplift.replace(np.nan, 'total', regex=True)
#         sales_uplift = pd.DataFrame.add(sales_uplift, fill_value='total')
        final_df = self.sqlContext.createDataFrame(sales_uplift,self.s_schema)  
#         final_df.show()
        final_df.show()
        final_df = final_df.withColumnRenamed('Total_uplift','adjusted_sales_uplift')
        print grouping_set
#         df_return1 = final_df.join(campain_df,joining_columns,'inner')
#         final = df_return1.withColumn("scr", df_return1.Total_Uplift / df_return1.campaign_cost)
        col_list = joining_columns + ['adjusted_sales_uplift']
        final = final_df.select(*col_list)
#         final = final.withColumn('scr',uplift/campain_cost)
        final.show()
        return final

    def units_uplift_percentage(self, grouping_set,joining_columns, column_set, measure):
        units_uplift = pd.DataFrame()
#         filtered_df = {}
        for key in self.data:
            df = self.sqlContext.table(self.config_dict['analysis_schema'] +'.' + key + '_group_df')
            df_control = df.filter(df.event_control_flag == 0)
            df_test = df.filter(df.event_control_flag == 1)
            if self.config_dict['uplift_method'] == "ancova":
                units_df = uplift_calculation.ancova(self.client,metric='units',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_period_var = 'pre_units',post_period_var = 'post_units' ,test_control_flg ='event_control_flag', confidence_Level_signi =0.95,confidence_Level_directional = 0.8,n_bin = 1)
                units_df[self.filtered_df[key]] = key
                units_df['grouping_level'] = self.filtered_df[key]
                control_post_spend = df_control.groupby().sum('post_units').collect()[0]
                test_pre_spend = df_test.groupby().sum('pre_units').collect()[0]
                control_pre_spend = df_control.groupby().sum('pre_units').collect()[0]
                control_post = getattr(control_post_spend,'sum(post_units)')
                test_pre = getattr(test_pre_spend,'sum(pre_units)')
                control_pre = getattr(control_pre_spend,'sum(pre_units)')
                units_df['expected_uplift'] = (control_post * (test_pre/control_pre))
            else:
                units_df = uplift_calculation.ratio(self.client, metric='units',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_vars = 'pre_units',post_vars = 'post_units' ,test_control_flg ='event_control_flag' ,confidence_Level_signi =0.95,confidence_Level_directional = 0.8)
                units_df[self.filtered_df[key]] = key
                print sales_df
                units_df['grouping_level'] = self.filtered_df[key]
                control_post_spend = df_control.groupby().sum('post_units').collect()[0]
                test_pre_spend = df_test.groupby().sum('pre_units').collect()[0]
                control_pre_spend = df_control.groupby().sum('pre_units').collect()[0]
                control_post = getattr(control_post_spend,'sum(post_units)')
                test_pre = getattr(test_pre_spend,'sum(pre_units)')
                control_pre = getattr(control_pre_spend,'sum(pre_units)')
                units_df['expected_uplift'] = ((control_post * test_pre)/control_pre)
#             print type(sales_df)
#             sales_uplift = sales_df.append(sales_uplift, ignore_index=True)
            units_uplift = pd.concat([units_df,units_uplift], ignore_index=True)
            print units_uplift
            print units_uplift.dtypes
        units_uplift['Metric'] = units_uplift['Metric'].astype('str')
        print units_uplift.dtypes
        units_uplift = units_uplift.replace(np.nan, 'total', regex=True)
#         sales_uplift = pd.DataFrame.add(sales_uplift, fill_value='total')
        final_df = self.sqlContext.createDataFrame(units_uplift,self.vis_uni_schema)  
#         final_df.show()
        final_df.show()
        final_df = final_df.withColumnRenamed('Total_uplift','units_uplift')
        print grouping_set
#         df_return1 = final_df.join(campain_df,joining_columns,'inner')
        final = final_df.withColumn("units_uplift_percentage", final_df.units_uplift / final_df.expected_uplift)
        col_list = joining_columns + ['units_uplift_percentage']
        final = final.select(*col_list)
#         final = final.withColumn('scr',uplift/campain_cost)
        final.show()
        return final
    
    def visits_uplift_percentage(self, grouping_set,joining_columns, column_set, measure):
        visits_uplift = pd.DataFrame()
#         filtered_df = {}
        for key in self.data:
            df = self.sqlContext.table(self.config_dict['analysis_schema'] +'.' + key + '_group_df')
            df_control = df.filter(df.event_control_flag == 0)
            df_test = df.filter(df.event_control_flag == 1)
            if self.config_dict['uplift_method'] == "ancova":
                visits_df = uplift_calculation.ancova(self.client,metric='visits',input_sparkdf = df,identifier = self.config_dict['identity_type_code'] ,pre_period_var = 'pre_visits',post_period_var = 'post_visits' ,test_control_flg ='event_control_flag', confidence_Level_signi =0.95,confidence_Level_directional = 0.8,n_bin = 1)
                visits_df[self.filtered_df[key]] = key
                visits_df['grouping_level'] = self.filtered_df[key]
                control_post_spend = df_control.groupby().sum('post_visits').collect()[0]
                test_pre_spend = df_test.groupby().sum('pre_visits').collect()[0]
                control_pre_spend = df_control.groupby().sum('pre_visits').collect()[0]
                control_post = getattr(control_post_spend,'sum(post_visits)')
                test_pre = getattr(test_pre_spend,'sum(pre_visits)')
                control_pre = getattr(control_pre_spend,'sum(pre_visits)')
                visits_df['expected_uplift'] = (control_post * (test_pre/control_pre))
            else:
                visits_df = uplift_calculation.ratio(self.client,metric='visits',input_sparkdf = df,identifier = self.config_dict['identity_type_code'] ,pre_vars = 'pre_visits',post_vars = 'post_visits' ,test_control_flg ='event_control_flag' ,confidence_Level_signi =0.95,confidence_Level_directional = 0.8)
                visits_df[self.filtered_df[key]] = key
                print visits_df
                visits_df['grouping_level'] = self.filtered_df[key]
                control_post_spend = df_control.groupby().sum('post_visits').collect()[0]
                test_pre_spend = df_test.groupby().sum('pre_visits').collect()[0]
                control_pre_spend = df_control.groupby().sum('pre_visits').collect()[0]
                control_post = getattr(control_post_spend,'sum(post_visits)')
                test_pre = getattr(test_pre_spend,'sum(pre_visits)')
                control_pre = getattr(control_pre_spend,'sum(pre_visits)')
                visits_df['expected_uplift'] = (control_post * (test_pre/control_pre))
            visits_uplift = pd.concat([visits_df,visits_uplift], ignore_index=True)
            print visits_uplift
            print visits_uplift.dtypes
        visits_uplift['Metric'] = visits_uplift['Metric'].astype('str')
        print visits_uplift.dtypes
        visits_uplift = visits_uplift.replace(np.nan, 'total', regex=True)
#         sales_uplift = pd.DataFrame.add(sales_uplift, fill_value='total')
        final_df = self.sqlContext.createDataFrame(visits_uplift,self.vis_uni_schema)  
#         final_df.show()
        final_df.show()
        final_df = final_df.withColumnRenamed('Total_uplift','visits_uplift')
        print grouping_set
        final = final_df.withColumn("visits_uplift_percentage", final_df.visits_uplift / final_df.expected_uplift)
        col_list = joining_columns + ['visits_uplift_percentage']
        final = final.select(*col_list)
#         final = final.withColumn('scr',uplift/campain_cost)
        final.show()
        return final

    def sales_uplift_percentage(self, grouping_set,joining_columns, column_set, measure):
        sales_uplift = pd.DataFrame()
#         filtered_df = {}
        for key in self.data:
            df = self.sqlContext.table(self.config_dict['analysis_schema'] +'.' + key + '_group_df')
            df_control = df.filter(df.event_control_flag == 0)
            df_test = df.filter(df.event_control_flag == 1)
            if self.config_dict['uplift_method'] == "ancova":
                sales_df = uplift_calculation.ancova(self.client,metric='sales',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_period_var = 'pre_spend',post_period_var = 'post_spend' ,test_control_flg ='event_control_flag', confidence_Level_signi =0.95,confidence_Level_directional = 0.8,n_bin = 1)
                sales_df[self.filtered_df[key]] = key
                sales_df['grouping_level'] = self.filtered_df[key]
                control_post_spend = df_control.groupby().sum('post_spend').collect()[0]
                test_pre_spend = df_test.groupby().sum('pre_spend').collect()[0]
                control_pre_spend = df_control.groupby().sum('pre_spend').collect()[0]
                control_post = getattr(control_post_spend,'sum(post_spend)')
                test_pre = getattr(test_pre_spend,'sum(pre_spend)')
                control_pre = getattr(control_pre_spend,'sum(pre_spend)')
                sales_df['expected_uplift'] = (control_post * (test_pre/control_pre))
            else:
                sales_df = uplift_calculation.ratio(self.client,metric='sales',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_vars = 'pre_spend',post_vars = 'post_spend' ,test_control_flg ='event_control_flag' ,confidence_Level_signi =0.95,confidence_Level_directional = 0.8)
                sales_df[self.filtered_df[key]] = key
                print sales_df
                sales_df['grouping_level'] = self.filtered_df[key]
                control_post_spend = df_control.groupby().sum('post_spend').collect()[0]
                test_pre_spend = df_test.groupby().sum('pre_spend').collect()[0]
                control_pre_spend = df_control.groupby().sum('pre_spend').collect()[0]
                control_post = getattr(control_post_spend,'sum(post_spend)')
                test_pre = getattr(test_pre_spend,'sum(pre_spend)')
                control_pre = getattr(control_pre_spend,'sum(pre_spend)')
                sales_df['expected_uplift'] = ((control_post * test_pre)/control_pre)
#             print type(sales_df)
#             sales_uplift = sales_df.append(sales_uplift, ignore_index=True)
            sales_uplift = pd.concat([sales_df,sales_uplift], ignore_index=True)
            print sales_uplift
            print sales_uplift.dtypes
        sales_uplift['Metric'] = sales_uplift['Metric'].astype('str')
        print sales_uplift.dtypes
        sales_uplift = sales_uplift.replace(np.nan, 'total', regex=True)
#         sales_uplift = pd.DataFrame.add(sales_uplift, fill_value='total')
        final_df = self.sqlContext.createDataFrame(sales_uplift,self.per_schema)  
#         final_df.show()
        final_df.show()
        final_df = final_df.withColumnRenamed('Total_uplift','sales_uplift')
        print grouping_set
#         df_return1 = final_df.join(campain_df,joining_columns,'inner')
        final = final_df.withColumn("sales_uplift_percentage", final_df.sales_uplift / final_df.expected_uplift)
        col_list = joining_columns + ['sales_uplift_percentage']
        final = final.select(*col_list)
#         final = final.withColumn('scr',uplift/campain_cost)
        final.show()
        return final
    
    def adjusted_uplift_percentage(self, grouping_set,joining_columns, column_set, measure):
        sales_uplift = pd.DataFrame()
#         filtered_df = {}
        for key in self.data:
            df = self.sqlContext.table(self.config_dict['analysis_schema'] +'.' + key + '_group_df')
            df_control = df.filter(df.event_control_flag == 0)
            df_test = df.filter(df.event_control_flag == 1)
            if self.config_dict['uplift_method'] == "ancova":
                sales_df = uplift_calculation.ancova(self.client,metric='sales',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_period_var = 'pre_spend',post_period_var = 'post_spend' ,test_control_flg ='event_control_flag', confidence_Level_signi =0.95,confidence_Level_directional = 0.8,n_bin = 1)
                sales_df[self.filtered_df[key]] = key
                sales_df['grouping_level'] = self.filtered_df[key]
                control_post_spend = df_control.groupby().sum('post_spend').collect()[0]
                test_pre_spend = df_test.groupby().sum('pre_spend').collect()[0]
                control_pre_spend = df_control.groupby().sum('pre_spend').collect()[0]
                control_post = getattr(control_post_spend,'sum(post_spend)')
                test_pre = getattr(test_pre_spend,'sum(pre_spend)')
                control_pre = getattr(control_pre_spend,'sum(pre_spend)')
                sales_df['expected_uplift'] = (control_post * (test_pre/control_pre))
            else:
                sales_df = uplift_calculation.ratio(self.client,metric='sales',input_sparkdf = df,identifier = self.identity_type_code,pre_vars = 'pre_spend',post_vars = 'post_spend' ,test_control_flg ='event_control_flag' ,confidence_Level_signi =0.95,confidence_Level_directional = 0.8)
                sales_df[self.filtered_df[key]] = key
                print sales_df
                sales_df['grouping_level'] = self.filtered_df[key]
                control_post_spend = df_control.groupby().sum('post_spend').collect()[0]
                test_pre_spend = df_test.groupby().sum('pre_spend').collect()[0]
                control_pre_spend = df_control.groupby().sum('pre_spend').collect()[0]
                control_post = getattr(control_post_spend,'sum(post_spend)')
                test_pre = getattr(test_pre_spend,'sum(pre_spend)')
                control_pre = getattr(control_pre_spend,'sum(pre_spend)')
                sales_df['expected_uplift'] = ((control_post * test_pre)/control_pre)
#             print type(sales_df)
#             sales_uplift = sales_df.append(sales_uplift, ignore_index=True)
            sales_uplift = pd.concat([sales_df,sales_uplift], ignore_index=True)
            print sales_uplift
            print sales_uplift.dtypes
        sales_uplift['Metric'] = sales_uplift['Metric'].astype('str')
        print sales_uplift.dtypes
        sales_uplift = sales_uplift.replace(np.nan, 'total', regex=True)
#         sales_uplift = pd.DataFrame.add(sales_uplift, fill_value='total')
        final_df = self.sqlContext.createDataFrame(sales_uplift,self.per_schema)  
#         final_df.show()
        final_df.show()
        final_df = final_df.withColumnRenamed('Total_uplift','sales_uplift')
        print grouping_set
#         df_return1 = final_df.join(campain_df,joining_columns,'inner')
        final = final_df.withColumn("adjusted_uplift_percentage", final_df.sales_uplift / final_df.expected_uplift)
        col_list = joining_columns + ['adjusted_uplift_percentage']
        final = final.select(*col_list)
#         final = final.withColumn('scr',uplift/campain_cost)
        final.show()
        return final
    
    def adjusted_uplift_per_customer(self, grouping_set,joining_columns, column_set, measure):
        sales_uplift = pd.DataFrame()
        for key in self.data:
            df = self.sqlContext.table(self.config_dict['analysis_schema'] +'.' + key + '_group_df')
            if self.config_dict['uplift_method'] == "ancova":
                sales_df = uplift_calculation.ancova(self.client,metric='sales',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_period_var = 'pre_spend',post_period_var = 'post_spend' ,test_control_flg ='event_control_flag', confidence_Level_signi =0.95,confidence_Level_directional = 0.8,n_bin = 1)
                sales_df[self.filtered_df[key]] = key
                sales_df['grouping_level'] = self.filtered_df[key]
            else:
                sales_df = uplift_calculation.ratio(self.client,metric='sales',input_sparkdf = df,identifier =self.config_dict['identity_type_code'], pre_vars = 'pre_spend',post_vars = 'post_spend' ,test_control_flg ='event_control_flag' ,confidence_Level_signi =0.95,confidence_Level_directional = 0.8)
                sales_df[self.filtered_df[key]] = key
                print sales_df
                sales_df['grouping_level'] = self.filtered_df[key]
            sales_uplift = pd.concat([sales_df,sales_uplift], ignore_index=True)
            print sales_uplift
            print sales_uplift.dtypes
        sales_uplift['Metric'] = sales_uplift['Metric'].astype('str')
        print sales_uplift.dtypes
        sales_uplift = sales_uplift.replace(np.nan, 'total', regex=True)
#         sales_uplift = pd.DataFrame.add(sales_uplift, fill_value='total')
        final_df = self.sqlContext.createDataFrame(sales_uplift,self.s_schema)  
#         final_df.show()
        final_df.show()
        final_df = final_df.withColumnRenamed('Total Test Cust','Total_Test_Cust')
        print grouping_set
#         df_return1 = final_df.join(campain_df,joining_columns,'inner')
        final = final_df.withColumn("adjusted_uplift_per_customer", final_df.Total_Uplift / final_df.Total_Test_Cust)
        col_list = joining_columns + ['adjusted_uplift_per_customer']
        final = final.select(*col_list)
#         final = final.withColumn('scr',uplift/campain_cost)
        final.show()
        return final
  
      
    def uplift_per_customer(self, grouping_set,joining_columns, column_set, measure):
        sales_uplift = pd.DataFrame()
#         filtered_df = {}
        for key in self.data:
            df = self.sqlContext.table(self.config_dict['analysis_schema'] +'.'+ key + '_group_df')
            if self.config_dict['uplift_method'] == "ancova":
                sales_df = uplift_calculation.ancova(self.client,metric='sales',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_period_var = 'pre_spend',post_period_var = 'post_spend' ,test_control_flg ='event_control_flag', confidence_Level_signi =0.95,confidence_Level_directional = 0.8,n_bin = 1)
                sales_df[self.filtered_df[key]] = key
                sales_df['grouping_level'] = self.filtered_df[key]
            else:
                sales_df = uplift_calculation.ratio(self.client,metric='sales',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_vars = 'pre_spend',post_vars = 'post_spend' ,test_control_flg ='event_control_flag' ,confidence_Level_signi =0.95,confidence_Level_directional = 0.8)
                sales_df[self.filtered_df[key]] = key
                print sales_df
                sales_df['grouping_level'] = self.filtered_df[key]
#             print type(sales_df)
#             sales_uplift = sales_df.append(sales_uplift, ignore_index=True)
            sales_uplift = pd.concat([sales_df,sales_uplift], ignore_index=True)
            print sales_uplift
            print sales_uplift.dtypes
        sales_uplift['Metric'] = sales_uplift['Metric'].astype('str')
        print sales_uplift.dtypes
        sales_uplift = sales_uplift.replace(np.nan, 'total', regex=True)
#         sales_uplift = pd.DataFrame.add(sales_uplift, fill_value='total')
        final_df = self.sqlContext.createDataFrame(sales_uplift,self.s_schema)  
#         final_df.show()
        final_df.show()
        final_df = final_df.withColumnRenamed('Total Test Cust','Total_Test_Cust')
        print grouping_set
#         df_return1 = final_df.join(campain_df,joining_columns,'inner')
        final = final_df.withColumn("uplift_per_customer", final_df.Total_Uplift / final_df.Total_Test_Cust)
        col_list = joining_columns + ['uplift_per_customer']
        final = final.select(*col_list)
#         final = final.withColumn('scr',uplift/campain_cost)
        final.show()
        return final
  
    def scr(self, grouping_set,joining_columns, column_set, measures):
        Cost = cost(self.client)
        campain_df = Cost.campaign_cost(grouping_set, column_set, measures)
        campain_df.show()
        sales_uplift = pd.DataFrame()
        for key in self.data:
#             print "1"
#             print self.data
#             print key
#             print self.filtered_df
#             print self.filtered_df[key]
            df = self.sqlContext.table(self.config_dict['analysis_schema'] +'.'+ key + '_group_df')
            df = df.withColumn("pre_spend", df["pre_spend"].cast(DoubleType())) 
            df = df.withColumn("post_spend", df["post_spend"].cast(DoubleType())) 
            df = df.replace(np.nan, '0')
#             df.show()
            if self.config_dict['uplift_method'] == "ancova":
#                 print "ancova"
                sales_df = uplift_calculation.ancova(self.client,metric='sales',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_period_var = 'pre_spend',post_period_var = 'post_spend' ,test_control_flg ='event_control_flag', confidence_Level_signi =0.95,confidence_Level_directional = 0.8,n_bin = 1)
                sales_df[self.filtered_df[key]] = key
                sales_df['grouping_level'] = self.filtered_df[key]
            else:
#                 print "ratio"
                sales_df = uplift_calculation.ratio(self.client,metric='sales',input_sparkdf = df,identifier = self.config_dict['identity_type_code'], pre_vars = 'pre_spend',post_vars = 'post_spend' ,test_control_flg ='event_control_flag' ,confidence_Level_signi =0.95,confidence_Level_directional = 0.8)
                sales_df[self.filtered_df[key]] = key
                print sales_df
                sales_df['grouping_level'] = self.filtered_df[key]
#             print type(sales_df)
#             sales_uplift = sales_df.append(sales_uplift, ignore_index=True)'
            sales_uplift = pd.concat([sales_df,sales_uplift], ignore_index=True)
            print sales_uplift
            print sales_uplift.dtypes
        sales_uplift['Metric'] = sales_uplift['Metric'].astype('str')
        print sales_uplift.dtypes
        sales_uplift = sales_uplift.replace(np.nan, 'total', regex=True)
#         sales_uplift = pd.DataFrame.add(sales_uplift, fill_value='total')
        final_df = self.sqlContext.createDataFrame(sales_uplift,self.s_schema)  
#         final_df.show()
        final_df.show()
        campain_df.show()
        print grouping_set
        df_return1 = final_df.join(campain_df,joining_columns,'inner')
        final = df_return1.withColumn("scr", df_return1.Total_Uplift / df_return1.campaign_cost)
        col_list = joining_columns + ['scr']
        final = final.select(*col_list)
#         final = final.withColumn('scr',uplift/campain_cost)
        final.show()
        return final