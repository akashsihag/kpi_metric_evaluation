from kpi.utilities import utils

from core_modules.utils import functions
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import col, trim, from_unixtime, unix_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, DoubleType, IntegerType
from pkg_resources import resource_string
import json

class BaseModule(object):
    """
    Parent class for all kpi modules
    """
    
#     __metaclass__ = utils.Singleton
    _dict = {}
    config_dict = {}
    
    def __init__(self, sc, client):
        if sc == None:
            print "sc not provided"
            return None
        
        if client == None:
            print "client not provided"
            return None
        
        self.client = client
        self.sc = sc
        self.sqlContext = HiveContext(sc)
        
    def read_config(self, config):
        BaseModule.config_dict['market'] = config['market_parameters']['market']
#         BaseModule.config_dict['variation_flag'] = config['market_parameters']['variation_flag']
        BaseModule.config_dict['config_variation'] = config['market_parameters']['config_variation']
        BaseModule.config_dict['data_variation'] = config['market_parameters']['data_variation']
        BaseModule.config_dict['targetting_mode'] = config['market_parameters']['targetting_mode']

        BaseModule.config_dict['adm_schema'] = config['database_parameters']['adm_schema']
        BaseModule.config_dict['analysis_schema'] = config['database_parameters']['analysis_schema']
        BaseModule.config_dict['segmentation_schema'] = config['database_parameters']['segmentation_schema']
        BaseModule.config_dict['output_db'] = config['database_parameters']['output_db']
        BaseModule.config_dict['write_mode'] = config['database_parameters']['write_mode']
        BaseModule.config_dict['flush_module_tables'] = config['database_parameters']['flush_module_tables']
        
        BaseModule.config_dict['output_prefix'] = config['database_parameters']['output_prefix']
        BaseModule.config_dict['output_suffix'] = config['database_parameters']['output_suffix']
        BaseModule.config_dict['final_table_name'] = config['database_parameters']['final_table_name']

        BaseModule.config_dict['table1'] = config['database_parameters']['table1']
        BaseModule.config_dict['table2'] = config['database_parameters']['table2']
        BaseModule.config_dict['table3'] = config['database_parameters']['table3']
        BaseModule.config_dict['offer_prod_redeem_mft'] = config['database_parameters']['offer_prod_redeem_mft']
        BaseModule.config_dict['event_dim'] = config['database_parameters']['event_dim']
        BaseModule.config_dict['offer_dim'] = config['database_parameters']['offer_dim']
        BaseModule.config_dict['card_dim'] = config['database_parameters']['card_dim']
        BaseModule.config_dict['card_loyalty_seg'] = config['database_parameters']['card_loyalty_seg']
        BaseModule.config_dict['card_lifestyle_seg'] = config['database_parameters']['card_lifestyle_seg']
        BaseModule.config_dict['card_pricesense_seg'] = config['database_parameters']['card_pricesense_seg']
        BaseModule.config_dict['transaction_item_mft'] = config['database_parameters']['transaction_item_mft']
        BaseModule.config_dict['transaction_basket_mft'] = config['database_parameters']['transaction_basket_mft']
        BaseModule.config_dict['prod_dim'] = config['database_parameters']['prod_dim']
        BaseModule.config_dict['identity_type_code'] = config['database_parameters']['identity_type_code']

        BaseModule.config_dict['event_date_format'] = config['formats']['event_date_format']
        BaseModule.config_dict['calendar_date_format'] = config['formats']['calendar_date_format']
        BaseModule.config_dict['transaction_date_format'] = config['formats']['transaction_date_format']
        BaseModule.config_dict['prod_redeem_date_format'] = config['formats']['prod_redeem_date_format']
        BaseModule.config_dict['transaction_date_time_format'] = config['formats']['transaction_date_time_format']

        BaseModule.config_dict['event_code'] = config['event_paramenters']['event_code']
        BaseModule.config_dict['event_start'] = config['event_paramenters']['event_start']
        BaseModule.config_dict['event_end'] = config['event_paramenters']['event_end']
        BaseModule.config_dict['pre_start'] = config['event_paramenters']['pre_start']
        BaseModule.config_dict['pre_end'] = config['event_paramenters']['pre_end']
        BaseModule.config_dict['post_start'] = config['event_paramenters']['post_start']
        BaseModule.config_dict['post_end'] = config['event_paramenters']['post_end']
        BaseModule.config_dict['seg_week'] = config['event_paramenters']['seg_week']
        BaseModule.config_dict['targ_date'] = config['event_paramenters']['targ_date']
        
        BaseModule.config_dict['event_weeks'] = int(config['event_paramenters']['event_weeks'])
        BaseModule.config_dict['pre_weeks'] = int(config['event_paramenters']['pre_weeks'])
        BaseModule.config_dict['post_weeks'] = int(config['event_paramenters']['post_weeks'])

        BaseModule.config_dict['external_resampling'] = config['uplift']['external_resampling']
        BaseModule.config_dict['resamp_file'] = config['uplift']['resamp_file']
        BaseModule.config_dict['resamp_type'] = config['uplift']['resamp_type']
        BaseModule.config_dict['clustering'] = config['uplift']['clustering']
        BaseModule.config_dict['uplift_level'] = config['uplift']['uplift_level']
        BaseModule.config_dict['uplift_method'] = config['uplift']['uplift_method']
        BaseModule.config_dict['reporting_level'] = config['uplift']['reporting_level']

        BaseModule.config_dict['schema'] = StructType([])
        
    def extract_client_df(self):
        client_dictionary = {
            'card_dim' : self.client.customers().data.select(
                col('card_code').alias(self.config_dict['identity_type_code']),
                'card_id',
                'card_birth_date',
                'card_termination_date',
                'card_address_valid_flag',
                'card_address_country_code',
                'card_analyse_now_suppress_flag',
                'card_suppress_flag'
            ),
            
            'desc_dt' : self.sqlContext.table('market_x_datalake.7_market_smartclub_members_c').select(
                F.concat(F.lit('0'),F.col('member_id')).alias("mem_id"),
                'deceased_date'
            ).filter(
                (F.col('deceased_date')=='00000000') | (F.col('deceased_date').isNull())
            ),
            
            'prod_dim' : self.client.products().data.select(
                'prod_code',
                'prod_hier_l20_code',
                'prod_desc'
            ),
            
            'store_dim' : self.client.stores().data.select(
                'banner_name',
                'store_code'
            ),
            
            'dur_period' : self.client.items(fisWeekId = self.config_dict['event_end'], weeks = self.config_dict['event_weeks']).data.select(
                col('card_code').alias(self.config_dict['identity_type_code']),
                'prod_code',
                col('net_spend_amt').cast(IntegerType()).alias('spend'),
                'prod_id',
                col('transaction_code').alias('transaction_fid'),
                'transaction_dttm',
                'fis_week_id'
            ).filter(
                col(self.config_dict['identity_type_code']).isNotNull()
                & (trim(col(self.config_dict['identity_type_code'])) != '')
                & (trim(col('prod_code')) != '')
                & col('fis_week_id').between(str(self.config_dict['event_start']), str(self.config_dict['event_end']))
            ),
            
            'pre_period' : self.client.items(fisWeekId = self.config_dict['pre_end'], weeks = self.config_dict['pre_weeks']).data.select(
                col('card_code').alias(self.config_dict['identity_type_code']),
                'prod_code',
                col('transaction_code').alias('transaction_fid'),
                'prod_id',
                col('net_spend_amt').cast(IntegerType()).alias('spend'),
                'fis_week_id'
            ).filter(
                col(self.config_dict['identity_type_code']).isNotNull() 
                & (trim(col(self.config_dict['identity_type_code'])) != '')
                & (trim(col('prod_code')) != '')
                & col('fis_week_id').between(str(self.config_dict['pre_start']), str(self.config_dict['pre_end']))
            ),
            
            'post_period' : self.client.items(fisWeekId = self.config_dict['post_end'], weeks = self.config_dict['post_weeks']).data.select(
                col('card_code').alias(self.config_dict['identity_type_code']),
                'prod_code',
                col('transaction_code').alias('transaction_fid'),
                'prod_id',
                col('net_spend_amt').cast(IntegerType()).alias('spend'),
                'fis_week_id'
            ).filter(
                col(self.config_dict['identity_type_code']).isNotNull()
                & (trim(col(self.config_dict['identity_type_code'])) != '')
                & (trim(col('prod_code')) != '')
                & col('fis_week_id').between(str(self.config_dict['post_start']), str(self.config_dict['post_end']))
            ),
            
            'dur_period_basket' : self.client.baskets(fisWeekId = self.config_dict['event_end'], weeks = self.config_dict['event_weeks']).data.select(
                col('card_code').alias(self.config_dict['identity_type_code']),
                col('basket_spend_amt').cast(IntegerType()).alias('spend'),
                col('basket_item_qty').cast(IntegerType()).alias('basket_item_qty'),
                col('basket_item_qty').alias('item'),
                col('transaction_code').alias('transaction_fid'),
                'transaction_dttm',
                'fis_week_id'
            ).filter(
                col(self.config_dict['identity_type_code']).isNotNull()
                & (trim(col(self.config_dict['identity_type_code'])) != '')
                & col('fis_week_id').between(str(self.config_dict['event_start']), str(self.config_dict['event_end']))
            ),
            
            'pre_period_basket' : self.client.baskets(fisWeekId = self.config_dict['pre_end'], weeks = self.config_dict['pre_weeks']).data.select(
                col('card_code').alias(self.config_dict['identity_type_code']),
                col('transaction_code').alias('transaction_fid'),
                col('basket_spend_amt').cast(IntegerType()).alias('spend'),
                col('basket_item_qty').cast(IntegerType()).alias('basket_item_qty'),
                col('basket_item_qty').alias('item'), 
                'fis_week_id'
            ).filter(
                col(self.config_dict['identity_type_code']).isNotNull()
                & (trim(col(self.config_dict['identity_type_code'])) != '')
                & col('fis_week_id').between(str(self.config_dict['pre_start']), str(self.config_dict['pre_end']))
            ),
            
            'post_period_basket' : self.client.baskets(fisWeekId = self.config_dict['post_end'], weeks = self.config_dict['post_weeks']).data.select(
                col('card_code').alias(self.config_dict['identity_type_code']),
                col('transaction_code').alias('transaction_fid'),
                col('basket_spend_amt').cast(IntegerType()).alias('spend'),
                col('basket_item_qty').cast(IntegerType()).alias('basket_item_qty'),
                'fis_week_id',
                col('basket_item_qty').alias('item')
            ).filter(
                col(self.config_dict['identity_type_code']).isNotNull() 
                & (trim(col(self.config_dict['identity_type_code'])) != '')
                & col('fis_week_id').between(str(self.config_dict['post_start']), str(self.config_dict['post_end']))
            ),
            
            'date_dim' : self.client.calendar().data
        }
        BaseModule._dict.update(client_dictionary)
      
    def extract_df(self):
        configfile = resource_string("kpi", 'configuration/' + self.config_dict['market'] + '/extractor_' + self.config_dict['targetting_mode'] + '.json')
        self.Config = json.loads(configfile)
        try:
            if self.Config.has_key("kpi_engine") and self.Config['kpi_engine'] != None:
                if self.Config["kpi_engine"].has_key('tables'):
                    for val in self.Config["kpi_engine"]['tables']:
                        if val.has_key('table_name') and val.has_key('df_name'):
                            print val['df_name']
                            try:
                                df = self.CreateDataFrame(val['table_name'], val['readDIS'])
                                if (val['table_name'].split(".")[1] != self.config_dict['offer_dim']):
                                    event_codes = self.config_dict['event_code'].split(",")
                                    df = df.filter(col('event_code').isin(event_codes)).drop(col('event_code'))
                                self.df_dict(val['df_name'], df, 'overwrite')
                            except Exception, e:
                                print e
                                self.df_dict(val['df_name'], None, 'overwrite')

                if self.Config["kpi_engine"].has_key('segmentation_tables'):
                    for val in self.Config["kpi_engine"]['segmentation_tables']:
                        if val.has_key('table_name') and val.has_key('df_name'):
                            print val['df_name']
                            try:
                                df = self.CreateDataFrame(val['table_name'], val['readDIS'])
                                df = df.filter(col('seg_week') == self.config_dict['seg_week']).drop(col('seg_week'))
                                self.df_dict(val['df_name'], df, 'overwrite')
                            except Exception, e:
                                print e
                                self.df_dict(val['df_name'], None, 'overwrite')
            else:
                raise Exception("Extract information not available in config file")
        except Exception, e:
            print "Exception Encountered %s" % str(e)
            return None

    def CreateDataFrame(self, path, DIS=None, source=None):
        if path == None:
            return None
        path1 = path.split(".")
        table = path1[1]
        Path = "/" + "/".join(path1[0].rsplit("_", 1)) + "/" + table
        # DataFrame=self.sqlContext.read.parquet(Path)
        DataFrame = self.sqlContext.read.table(path)
        if DIS == None:
            return DataFrame
        elif 'columns' in DIS.keys():  # Need to read specific columns
            columns = DIS["columns"]
            # Get input columns to read for from the source file, note that we
            # might not want to read all columns
            if isinstance(DIS["columns"], dict) is False:
                col_list = [i for i in columns]
                finaldf = DataFrame.select([DataFrame[c] for c in col_list])
            else:
                finaldf = DataFrame.select([DataFrame[c].alias(columns[c])
                                            for c in columns.keys()])
            return finaldf

    # method acts as a trigger to initiate all child module KPIs
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
        
        module_df = self.sqlContext.createDataFrame(
            [[''] * len(joining_columns)], joining_columns
        ).filter(
            col(joining_columns[0]) != ''
        )
        
        for i in range(len(measures)):
            method = getattr(self, measures[i])
            print(measures[i])
            kpi_df = method(grouping_set, column_set, measures[i])
            if utils.valid_df(kpi_df):
                if(i == 0):
                    module_df = kpi_df
                else:
                    module_df = module_df.join(kpi_df, joining_columns, 'outer')
        module_df = module_df.fillna('0', measures)
        return module_df
    
    def flush_module_tables(self):
        for module_name in self.config['module_names']:
            if (self.config['module_names'][module_name] == 'Y'):
                table_name = self.config_dict['output_suffix'] + '_' + module_name + '_' + self.config_dict['output_prefix']
                utils.flush_table(self.sqlContext, self.config_dict['output_db'], table_name)
    
    def get_empty_df(self):
        df = self.sqlContext.createDataFrame(self.sc.emptyRDD(), self.config_dict['schema'])
        return df
    
    def df_dict(self, name = None, df = None, overwrite = False):
        if name is None:
            return BaseModule._dict
        try:
            if overwrite and df:
                BaseModule._dict[name] = df
            elif df and name not in BaseModule._dict:
                BaseModule._dict[name] = df
            elif name not in BaseModule._dict and name == 'empty_df':
                BaseModule._dict[name] = self.get_empty_df()
            BaseModule._dict[name].cache()
            return BaseModule._dict[name]
        except Exception as e:
            print (e)
            return None
    
    def remove_df(self, name):
        if name in BaseModule._dict:
            BaseModule._dict.pop(name)
