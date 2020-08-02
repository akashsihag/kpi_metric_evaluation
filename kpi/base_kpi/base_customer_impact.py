from kpi.utilities import utils
from kpi.base_kpi.base_module import BaseModule

from pyspark.sql.functions import col
from pyspark.sql.functions import from_unixtime, unix_timestamp
from pyspark.sql.types import DoubleType, StringType
import pyspark.sql.functions as F

class BaseCustomerImpact(BaseModule):
    """
    customer_impact module
    """
    def __init__(self, sc, client):
        super(BaseCustomerImpact, self).__init__(sc, client)
        
        self.detail_seg_df = self.df_dict(self.config_dict['data_variation']).filter(col('contact_stage_code') == 'EXP').drop('prod_code')
        
        self.detail_offer_prod = self.detail_seg_df.join(self.df_dict('table3'), 'offer_code', 'left_outer')
        
        self.dur_period_bi = self.df_dict('dur_period').select(self.config_dict['identity_type_code'], 'prod_code').distinct()
        
        self.detail_offer_prod_dur = self.detail_offer_prod.join(self.dur_period_bi, [self.config_dict['identity_type_code'], 'prod_code'])
        
        self.post_count_df = self.df_dict('post_period').groupby(self.config_dict['identity_type_code'], 'prod_code').count()

    def trialist(self, grouping_set, column_set, measure):
        df = self.detail_offer_prod_dur.join(
            self.post_count_df,
            [self.config_dict['identity_type_code'],
             'prod_code'], 'left_outer'
        ).filter(
            col('count').isNull()
        )
        
        trialist_df = utils.distinct_count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return trialist_df
    
    def repeaters(self, grouping_set, column_set, measure):
        post_one_time_df = self.post_count_df.filter(
            col('count') == 1
        ).drop('count')
        
        df = self.detail_offer_prod_dur.join(
            post_one_time_df,
            [self.config_dict['identity_type_code'], 'prod_code']
        )
        
        repetors_df = utils.distinct_count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        
        return repetors_df
    
    def adopters(self, grouping_set, column_set, measure):
        post_mul_time_df = self.post_count_df.filter(
            col('count') > 1
        ).drop('count')
        
        df = self.detail_offer_prod_dur.join(
            post_mul_time_df,
            [self.config_dict['identity_type_code'], 'prod_code']
        )
        
        adopters_df = utils.distinct_count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return adopters_df
    
    def buy_in_category_but_not_product(self, grouping_set, column_set, measure):
        exp_df = self.detail_offer_prod
#         .withColumnRenamed('prod_code', 'featured_prod_code')
        
#         print 'exp_df'
#         exp_df.cache()
#         print exp_df.count()
#         exp_df.show()
        
        prod_dim_df = self.df_dict('prod_dim')
#         prod_dim_df.cache()
#         prod_dim_df.show()
        
        exp_cat_df = exp_df.join(
            prod_dim_df,
            'prod_code'
            'left_outer'
        )
        
        exp_cat_df = exp_cat_df.withColumnRenamed('prod_code', 'featured_prod_code')
        
#         print 'exp_cat_df'
#         exp_cat_df.cache()
#         print exp_cat_df.count()
#         exp_cat_df.show()
        
        pre_prod_df = self.df_dict('pre_period').join(
            self.df_dict('prod_dim'),
            'prod_code',
            'left_outer'
        )
        
#         print 'pre_prod_df'
#         pre_prod_df.cache()
#         print pre_prod_df.count()
#         pre_prod_df.show()
        
        post_prod_df = self.df_dict('post_period').join(
            self.df_dict('prod_dim'),
            'prod_code',
            'left_outer'
        )
        
#         print 'post_prod_df'
#         post_prod_df.cache()
#         print post_prod_df.count()
#         post_prod_df.show()
        
        pre_post_df = pre_prod_df.select(
            self.config_dict['identity_type_code'],
            'prod_hier_l20_code'
        ).intersect(
            post_prod_df.select(
                self.config_dict['identity_type_code'],
                'prod_hier_l20_code'
            )
        )
        
#         print 'pre_post_df'
#         pre_post_df.cache()
#         print pre_post_df.count()
#         pre_post_df.show()
        
        pre_post_exp_cat_df = exp_cat_df.join(
            pre_post_df,
            [self.config_dict['identity_type_code'], 'prod_hier_l20_code']
        )
        
#         print 'pre_post_exp_cat_df'
#         pre_post_exp_cat_df.cache()
#         print pre_post_exp_cat_df.count()
#         pre_post_exp_cat_df.show()

        df = pre_post_exp_cat_df.filter(
            pre_post_exp_cat_df.featured_prod_code != pre_post_exp_cat_df.prod_code
        )
        
#         print 'df'
#         df.cache()
#         print df.count()
#         df.show()
        
#         .drop('channel_code')
        
        buy_in_category_but_not_product_df = utils.count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return buy_in_category_but_not_product_df
    
    # for pre-period 
    def pre_average_frequency_of_purchase(self, grouping_set, column_set, measure):
        df = self.detail_offer_prod.drop('fis_week_id').drop('fis_year_id').drop('prod_code').join(
            self.df_dict('pre_period').drop(),
            [self.config_dict['identity_type_code']]
        )
        
        customer_count = df.select('prsn_code').distinct().count()
        average_frequency_of_purchase_df = utils.distinct_count_average(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            'transaction_fid',
            customer_count
        )
        return average_frequency_of_purchase_df
    
    # for dur-period
    def dur_average_frequency_of_purchase(self, grouping_set, column_set, measure):
        df = self.detail_offer_prod.drop('fis_week_id').drop('fis_year_id').drop('prod_code').join(
            self.df_dict('dur_period').drop(),
            [self.config_dict['identity_type_code']]
        )
        
        customer_count = df.select('prsn_code').distinct().count()
        average_frequency_of_purchase_df = utils.distinct_count_average(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            'transaction_fid',
            customer_count
        )
        return average_frequency_of_purchase_df
    
    # for post-period
    def post_average_frequency_of_purchase(self, grouping_set, column_set, measure):
        df = self.detail_offer_prod.drop('fis_week_id').drop('fis_year_id').drop('prod_code').join(
            self.df_dict('post_period'),
            [self.config_dict['identity_type_code']]
        )
        
        customer_count = df.select('prsn_code').distinct().count()
        average_frequency_of_purchase_df = utils.distinct_count_average(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            'transaction_fid',
            customer_count
        )
        return average_frequency_of_purchase_df
  
    def lapsed_buyers(self, grouping_set, column_set, measure):
        df = self.detail_offer_prod.join(
            self.df_dict('pre_period').select(self.config_dict['identity_type_code'], 'prod_code'),
            [self.config_dict['identity_type_code'], 'prod_code']
        )
        
        df = df.join(
            self.df_dict('post_period').select(self.config_dict['identity_type_code'], 'prod_code', 'transaction_fid'),
            [self.config_dict['identity_type_code'], 'prod_code'],
            'left_outer'
        ).filter(
            col('transaction_fid').isNull()
        )
        
        lapsed_buyers_df = utils.distinct_count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return lapsed_buyers_df
    
    def retained_hhs(self, grouping_set, column_set, measure):
        df = self.detail_offer_prod.join(
            self.df_dict('pre_period').select(self.config_dict['identity_type_code'], 'prod_code'),
            [self.config_dict['identity_type_code'], 'prod_code']
        )
        
        df = df.join(
            self.df_dict('post_period').select(self.config_dict['identity_type_code'], 'prod_code'),
            [self.config_dict['identity_type_code'], 'prod_code']
        )
        
        retained_hhs = utils.distinct_count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return retained_hhs
    
    # not used in  market
    # at overall_level trancation_code and rdm trans/ total-trans exp customers
    def average_basket_with_offer(self, grouping_set, column_set, measure):
        # transactions
        transaction_df = self.df_dict('dur_period').drop('fis_week_id').drop('fis_year_id').withColumn(
            'timestamp', 
            unix_timestamp(
                col('transaction_dttm'),
                self.config_dict['transaction_date_format']
            )
        ).drop('transaction_dttm')
        
        #redeemed transactions
        rdm = self.df_dict(self.config_dict['data_variation']).filter(col('contact_stage_code') == "RDM").withColumn(
            'timestamp',
            unix_timestamp(
                col('contact_cust_detail_dttm'), 
                self.config_dict['prod_redeem_date_format'])
        ).drop('contact_cust_detail_dttm').select(self.config_dict['identity_type_code'], 'prod_code', 'timestamp')
        
        rdm_transaction = transaction_df.join(rdm,
                                              [self.config_dict['identity_type_code'], 'prod_code', 'timestamp']
                                             ).select('transaction_fid')
        
        #exposed transactions
        exp = self.df_dict(self.config_dict['data_variation']).filter(col('contact_stage_code') == "EXP").select(self.config_dict['identity_type_code']).distinct()
        
        exp_transaction = transaction_df.join(exp, self.config_dict['identity_type_code']).select('transaction_fid')
        
        final_df = self.sqlContext.createDataFrame(
            [["overall"]],
            ["grouping_level"]
        )
        
        final_df = final_df.withColumn(
            measure,
            F.lit(F.lit(
                rdm_transaction.distinct().count()
            ).cast(DoubleType())/F.lit(
                exp_transaction.distinct().count()
            ).cast(DoubleType())).cast(StringType())
        )
        
        for col_name in column_set:
            final_df = final_df.withColumn(col_name, F.lit('total'))
            
        final_df = final_df.withColumn('grouping_level', F.lit('overall'))
        
        average_basket_with_offer_df = final_df.withColumn(
            measure,
            final_df[measure].cast(StringType())
        )
            
        return average_basket_with_offer_df