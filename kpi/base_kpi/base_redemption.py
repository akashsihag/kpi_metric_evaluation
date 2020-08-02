from kpi.utilities import utils
from kpi.base_kpi.base_module import BaseModule

from pyspark.sql.functions import col, lower, lit
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StringType
import pyspark.sql.functions as func

class BaseRedemption(BaseModule):
    """
    redemption module
    """
    def __init__(self, sc, client):
        super(BaseRedemption, self).__init__(sc, client)
            
        self.base_redemption_df = self.df_dict(self.config_dict['data_variation'])
        
        self.df_redeem = self.base_redemption_df.filter(col('contact_stage_code') == 'RDM')
        self.df_exp = self.base_redemption_df.filter(col('contact_stage_code') == 'EXP').select(self.config_dict['identity_type_code'], 'offer_code')
        
        self.df_redemptions = self.df_dict('empty_df')
        self.df_prsn = self.df_dict('empty_df')
        
        self.table3 = self.df_dict('table3')
        
        self.dur_period = self.df_dict('dur_period')
        
        self.universe_df = self.df_dict('card_dim')
        self.desc_dt_df = self.df_dict('desc_dt')
        
   # QA with Archit and Sharang
    def hh_redemeers(self, grouping_set, column_set, measure):
        if not self.df_prsn.head(1):
            df_final = utils.distinct_count(
                self.sqlContext,
                self.df_redeem,
                grouping_set,
                column_set,
                measure,
                self.config_dict['identity_type_code']
            )
            
            self.df_prsn = df_final
        else:
            df_final = self.df_prsn
        return df_final
    
    # QA with Archit and Sharang
    def number_of_redemptions(self, grouping_set, column_set, measure):
        if not self.df_redemptions.head(1):
            df_offer = utils.count(
                self.sqlContext,
                self.df_redeem,
                grouping_set,
                column_set,
                measure,
                'offer_code'
            )
            
            self.df_redemptions = df_offer
        else:
            df_offer = self.df_redemptions
        return df_offer
    
    # correct_redemeers vs valid_redemption ==> 
    # distinct test customer redeemed
#     def correct_redemeers(self, grouping_set, column_set, measure):
#         df_correct = self.base_redemption_df.filter(
#             (col('event_control_flag') == 'N')
#             & (col('contact_stage_code') == 'RDM')
#         )
        
#         df_final = utils.distinct_count(
#             self.sqlContext,
#             df_correct,
#             grouping_set,
#             column_set,
#             measure,
#             self.config_dict['identity_type_code']
#         )
#         return df_final
  
    # QA with Archit and Sharang 
    def correct_redemeers(self, grouping_set, column_set, measure):
        df_correct = self.base_redemption_df.filter(
            col('contact_stage_code') == 'EXP'
        ).select(
            self.config_dict['identity_type_code'],
            'offer_code'
        ).dropDuplicates()
        
#         print 'df_correct'
#         df_correct.cache()
#         print df_correct.count()
#         df_correct.show()
        
        
#         print 'self.df_redeem'
#         self.df_redeem.cache()
#         print self.df_redeem.count()
#         print self.df_redeem.show()
        
        df = self.df_redeem.join(
            df_correct,
            [self.config_dict['identity_type_code'], 'offer_code'],
            'inner'
        )
        
#         print 'df'
#         df.cache()
#         print df.count()
#         print df.select('prsn_code').count()
#         df.show()
        
        df_final = utils.distinct_count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return df_final
    
#     def valid_redemption(self, grouping_set, column_set, measure):
#         df_correct = self.base_redemption_df.filter(
#             col('contact_stage_code') == 'EXP'
#         ).select(
#             self.identity_type_code,
#             'offer_code'
#         ).dropDuplicates()
        
#         df = self.base_redemption_df.filter(
#             col('contact_stage_code') == 'RDM'
#         ).join(
#             df_correct,
#             [self.identity_type_code, 'offer_code'],
#             'inner'
#         )
        
#         df_final = utils.distinct_count(
#             self.sqlContext,
#             df,
#             grouping_set,
#             column_set,
#             measure,
#             self.identity_type_code
#         )
#         return df_final

    # QA with Archit and Sharang 
    # its redemptions and hence should be count instead of disctinct (count difference is there)
    def valid_redemption(self, grouping_set, column_set, measure):
        df_correct = self.base_redemption_df.filter(
            col('contact_stage_code') == 'EXP'
        ).select(
            self.config_dict['identity_type_code'],
            'offer_code'
        ).dropDuplicates()
        
        df = self.base_redemption_df.filter(
            col('contact_stage_code') == 'RDM'
        ).join(
            df_correct,
            [self.config_dict['identity_type_code'], 'offer_code'],
            'inner'
        )
        
        df_final = utils.count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return df_final
    
    # QA with Archit and Sharang 
    def paper_redemeers(self,grouping_set,column_set,measure):
        df = self.df_redeem.filter(
            lower(col('channel_code')).like("%paper%")
        )
        
        df_final = utils.distinct_count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return df_final
    
    # QA with Archit and Sharang 
    def digital_redemeers(self, grouping_set, column_set, measure):
        df = self.df_redeem.filter(
            lower(col('channel_code')).like("%digital%")
        )
        
        df_final = utils.distinct_count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return df_final

    # Act stage KPI not used in market
    # slot level not possible since ACT stage don't have offer_rank_nums
    def paper_trigered(self, grouping_set, column_set, measure):
        df = self.base_redemption_df.filter(
            (lower(col('channel_code')).like('%paper%')) 
            & (col('contact_stage_code') == 'ACT')
        )
        
        df_final = utils.count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            'offer_code'
        )
        return df_final
    
    # Act stage KPI not used in market
    # slot level not possible since ACT stage don't have offer_rank_num
    def digital_trigered(self, grouping_set, column_set, measure):
        df = self.base_redemption_df.filter(
            (lower(col('channel_code')).like('%digital%'))
            & (col('contact_stage_code') == 'ACT')
        )
        df_final = utils.count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            'offer_code'
        )
        return df_final
    
    # QA with Archit and Sharang
    # this is not participation this is penetration if we divide it by participation base
    def mailed_penetration(self, grouping_set, column_set, measure):
        df_customer = self.table3.select(
            'prod_code'
        ).dropDuplicates()
#         print 'df_customer'
#         df_customer.cache()
#         print df_customer.count()
        
        
        # join with exp stage
        df_transaction = self.dur_period.select(
            self.config_dict['identity_type_code'],
            'prod_code'
        ).dropDuplicates()
        
#         print 'df_transaction'
#         df_transaction.cache()
#         print df_transaction.count()
        
        df_trans = df_customer.join(df_transaction, ['prod_code']).select(self.config_dict['identity_type_code'])
        
#         print 'df_trans'
#         df_trans.cache()
#         print df_trans.count()
        
#         df_trans1 = df_trans.select(
#             self.identity_type_code
#         ).dropDuplicates()
        
        exp_df = self.base_redemption_df.filter(col('contact_stage_code') == 'EXP')
#         print 'exp_df'
#         exp_df.cache()
#         print exp_df.count()
        
        
        df_prod = df_trans.join(exp_df, self.config_dict['identity_type_code'])
#         print 'df_prod'
#         df_prod.cache()
#         print df_prod.count()
        
        
        purchase_df = utils.distinct_count(
            self.sqlContext,
            df_prod,
            grouping_set,
            column_set,
            'purchase',
            self.config_dict['identity_type_code']
        )
#         print 'purchase_df'
#         purchase_df.cache()
#         print purchase_df.count()
        
        exp_df = utils.distinct_count(
            self.sqlContext,
            exp_df,
            grouping_set,
            column_set,
            'exposed',
            self.config_dict['identity_type_code']
        )
#         print 'exp_df'
#         exp_df.cache()
#         print exp_df.count()
        
        group_set = column_set + ['grouping_level']
        
        df_final = exp_df.join(purchase_df, group_set)
        
        df_final = df_final.withColumn(
            measure,
            df_final.purchase/df_final.exposed
        )
        
        df_final = df_final.drop('purchase')
        df_final = df_final.drop('exposed')
        
        df_final = df_final.withColumn(
            measure,
            df_final[measure].cast(StringType())
        )
        return df_final
    
    # using card_dim_c as universe as we don't have card_dim_h in market_x_easl
    # metric at overall only
    def overall_penetration(self, grouping_set, column_set, measure):
        df_products = self.table3.select(
            'prod_code'
        ).dropDuplicates()
        
#         print 'df_products'
#         df_products.cache()
#         print df_products.count()
        
        df_transaction = self.dur_period.select(
            self.config_dict['identity_type_code'],
            'prod_code'
        ).dropDuplicates()
        
#         print 'df_transaction'
#         df_transaction.cache()
#         print df_transaction.count()
        
        df_offer_products = df_products.join(df_transaction, ['prod_code'])
#         print 'df_offer_products'
#         df_offer_products.cache()
#         print df_offer_products.count()
        
        df_trans = df_offer_products.select(self.config_dict['identity_type_code'])
#         print 'df_trans'
#         df_trans.cache()
#         print df_trans.count()
        
        universe_df = self.universe_df.withColumn("sub_cc", F.substring(F.col(self.config_dict['identity_type_code']), 2, 4))
        new_cust = universe_df.where(F.col("sub_cc").isin("0000","0181","5020","5027","6066","1166","2345") == False)
        new_cust = new_cust.where(F.col("card_termination_date").isNull())
        new_cust = new_cust.where(F.col("card_address_valid_flag")=="N")
        new_cust = new_cust.where(F.col("card_address_country_code").isin("NO"))
        new_cust = new_cust.where(F.col("card_analyse_now_suppress_flag") == "")
        new_cust = new_cust.where(F.col("card_suppress_flag") == "N")
        
#         print 'universe_df'
#         universe_df.cache()
#         print universe_df.count()
        
        new_cust = new_cust.join(self.desc_dt_df, self.desc_dt_df["mem_id"] == new_cust[self.config_dict['identity_type_code']])
        new_cust = new_cust.select(self.config_dict['identity_type_code'])
        
#         print 'new_cust'
#         new_cust.cache()
#         print new_cust.count()
        
        final_df = self.sqlContext.createDataFrame(
            [["overall"]],
            ["grouping_level"]
        )
        
        final_df = final_df.withColumn(
            measure,
            F.lit(F.lit(
                df_trans.distinct().count()
            ).cast(DoubleType())/F.lit(
                new_cust.distinct().count()
            ).cast(DoubleType())).cast(StringType())
        )
        
#         print 'final_df'
#         final_df.cache()
#         print final_df.count()
        
        for col_name in column_set:
            final_df = final_df.withColumn(col_name, F.lit('total'))
            
        final_df = final_df.withColumn('grouping_level', F.lit('overall'))
        
        final_df = final_df.withColumn(
            measure,
            final_df[measure].cast(StringType())
        )
        return final_df
    
    # QA with Archit and Sharang
    def mailed_participation(self, grouping_set, column_set, measure):
        df_control = self.table3.select(
            'prod_code'
        ).dropDuplicates()
        
#         print 'df_control'
#         df_control.cache()
#         print df_control.count()
        
        df_transaction = self.dur_period.select(
            self.config_dict['identity_type_code'],
            'prod_code'
        ).dropDuplicates()
        
#         print 'df_transaction'
#         df_transaction.cache()
#         print df_transaction.count()
        
        df_trans = df_control.join(df_transaction,['prod_code'])
#         print 'df_trans'
#         df_trans.cache()
#         print df_trans.count()
        
        df_mail = self.base_redemption_df.drop('prod_code')
        
        df_mailed = df_mail.filter(col('contact_stage_code') == 'EXP')
#         print 'df_mailed'
#         df_mailed.cache()
#         print df_mailed.count()
        
        df_prod = df_trans.join(df_mailed, self.config_dict['identity_type_code'])
#         print 'df_prod'
#         df_prod.cache()
#         print df_prod.count()
        
        df_final=utils.distinct_count(
            self.sqlContext,
            df_prod,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return df_final
    
    # QA with Archit and Sharang
    def redemption_rate(self, grouping_set, column_set, measure):
        redemeed = self.base_redemption_df.filter(
            col('contact_stage_code') == 'RDM'
        )
#         print 'redemeed'
#         redemeed.cache()
#         print redemeed.count()
        
        mailed = self.base_redemption_df.filter(
            col('contact_stage_code') == 'EXP'
        )
#         print 'mailed'
#         mailed.cache()
#         print mailed.count()
        
        redem_coupon = utils.count(
            self.sqlContext,
            redemeed,
            grouping_set,
            column_set,
            'redem_coupon',
            'offer_code'
        )
#         print 'redem_coupon'
#         redem_coupon.cache()
#         print redem_coupon.count()
        
        mailed_coupon = utils.count(
            self.sqlContext,
            mailed,
            grouping_set,
            column_set,
            'mailed_coupon',
            'offer_code'
        )
#         print 'mailed_coupon'
#         mailed_coupon.cache()
#         print mailed_coupon.count()
        
        group_set = column_set + ['grouping_level']
        df_redem_mailed = redem_coupon.join(mailed_coupon, group_set)
#         print 'df_redem_mailed'
#         df_redem_mailed.cache()
#         print df_redem_mailed.count()
        
        df_final = df_redem_mailed.withColumn(
            measure,
            df_redem_mailed.redem_coupon/df_redem_mailed.mailed_coupon
        )
#         print 'df_final'
#         df_final.cache()
#         print df_final.count()
        
        df_redemption_rate = df_final.drop('redem_coupon')
        df_redemption_rate = df_redemption_rate.drop('mailed_coupon')
        
        df_redemption_rate = df_redemption_rate.withColumn(
            measure,
            df_redemption_rate[measure].cast(StringType())
        )
        
        return df_redemption_rate
    
    # QA with Archit and Sharang
    def response_rate(self,grouping_set,column_set,measure):
        redemeed = self.base_redemption_df.filter(col('contact_stage_code') == 'RDM')
        mailed = self.base_redemption_df.filter(col('contact_stage_code') == 'EXP')
        
        redem_coupon = utils.distinct_count(
            self.sqlContext,
            redemeed,
            grouping_set,
            column_set,
            'redem_coupon',
            self.config_dict['identity_type_code']
        )
#         print 'redem_coupon'
#         redem_coupon.cache()
#         print redem_coupon.count()
        
        mailed_coupon = utils.distinct_count(
            self.sqlContext,
            mailed,
            grouping_set,
            column_set,
            'mailed_coupon',
            self.config_dict['identity_type_code']
        )
#         print 'mailed_coupon'
#         mailed_coupon.cache()
#         print mailed_coupon.count()
        
        group_set = column_set + ['grouping_level']
        
        df_redem_mailed = redem_coupon.join(mailed_coupon, group_set)
#         print 'df_redem_mailed'
#         df_redem_mailed.cache()
#         print df_redem_mailed.count()
        
        df_final = df_redem_mailed.withColumn(
            measure,
            df_redem_mailed.redem_coupon/df_redem_mailed.mailed_coupon
        )
#         print 'df_final'
#         df_final.cache()
#         print df_final.count()
        
        df_response_rate = df_final.drop('redem_coupon')
        df_response_rate = df_response_rate.drop('mailed_coupon')
        
        df_response_rate = df_response_rate.withColumn(
            measure,
            df_response_rate[measure].cast(StringType())
        )
        return df_response_rate
    
   # QA with Archit and Sharang 
    def coupon_redeem_per_hh(self,grouping_set,column_set,measure):
        if not self.df_prsn.head(1):
            df_prsn = self.hh_redemeers(
                grouping_set,
                column_set,
                'hh_redemeers'
            )
            self.df_prsn=df_prsn
        else:
            df_prsn=self.df_prsn
        
        if not self.df_redemptions.head(1):
            df_offer = self.number_of_redemptions(
                grouping_set,
                column_set,
                'number_of_redemptions'
            )
            self.df_redemptions = df_offer
#             print 'self.df_redemptions'
#             self.df_redemptions.cache()
#             print self.df_redemptions.count()
        else:
            df_offer = self.df_redemptions
        group_set = column_set + ['grouping_level']
        df_return1 =df_prsn.join(df_offer, group_set, 'outer')
#         print 'df_return1'
#         df_return1.cache()
#         df_return1.show()
        df_return1 = df_return1.withColumn("hh_redemeers",
                                           df_return1["hh_redemeers"].cast(DoubleType())
                                          )
        df_return1= df_return1.withColumn("number_of_redemptions",
                                          df_return1["number_of_redemptions"].cast(DoubleType())
                                         )
        df_final= df_return1.withColumn(
            measure, 
            df_return1.number_of_redemptions/df_return1.hh_redemeers
        )
        
        df_coupon_redeem_per_hh = df_final.drop('hh_redemeers')
        df_coupon_redeem_per_hh = df_coupon_redeem_per_hh.drop('number_of_redemptions')

        df_coupon_redeem_per_hh = df_coupon_redeem_per_hh.withColumn(
            measure,
            df_coupon_redeem_per_hh[measure].cast(StringType())
        )
        
        return df_coupon_redeem_per_hh
    
    # moved to phase 2 after discussion with Charu
    def index_vs_mailed(self,grouping_set,column_set,measure):
        redemeed = self.base_redemption_df.filter(col('contact_stage_code') == 'RDM')
        mailed = self.base_redemption_df.filter(col('contact_stage_code') == 'EXP')
        
        redem_coupon = utils.distinct_count(
            self.sqlContext,
            redemeed,
            grouping_set,
            column_set,
            'redem_coupon',
            self.config_dict['identity_type_code']
        )
        
        mailed_coupon = utils.distinct_count(
            self.sqlContext,
            mailed,
            grouping_set,
            column_set,
            'mailed_coupon',
            self.config_dict['identity_type_code']
        )
        
        mailed_1 = mailed.agg(
            func.countDistinct(self.config_dict['identity_type_code']).alias('total_mailed')
        )
        
        mailed_2 = mailed_1.withColumn('flag', lit(1))
        group_set = column_set + ['grouping_level']
        df_redem_mailed = redem_coupon.join(mailed_coupon,group_set)
        df_redem_mailed_flag= df_redem_mailed.withColumn('flag', lit(1))
        
        df_redem_mailed_flag.cache()
        df_redem_mailed_flag.show()
        
        redemeed_1 = redemeed.agg(
            func.countDistinct(self.config_dict['identity_type_code']).alias('total_redem')
        )
        
        redemeed_2 = redemeed_1.withColumn('flag', lit(1))
        df_mailed_flag = df_redem_mailed_flag.join(mailed_2,['flag'])
        df_redemeed_flag = df_mailed_flag.join(redemeed_2,['flag'])
        df_mailed_index= df_redemeed_flag.withColumn(
            'mailed_index',
            df_redemeed_flag.mailed_coupon/df_redemeed_flag.total_mailed
        )
        df_redemeed_index = df_mailed_index.withColumn(
            'redemeed_index',
            df_mailed_index.redem_coupon/df_mailed_index.total_redem
        )
    
        df_index_mailed = df_redemeed_index.withColumn(
            measure,
            df_redemeed_index.redemeed_index/df_redemeed_index.mailed_index
        )
        
        df_index_mailed = df_index_mailed.drop('redem_coupon')
        df_index_mailed = df_index_mailed.drop('mailed_coupon')
        df_index_mailed = df_index_mailed.drop('flag')
        df_index_mailed = df_index_mailed.drop('total_mailed')
        df_index_mailed = df_index_mailed.drop('total_redem')
        df_index_mailed = df_index_mailed.drop('mailed_index')
        df_index_mailed = df_index_mailed.drop('redemeed_index')

        df_index_mailed = df_index_mailed.withColumn(
            measure,
            df_index_mailed[measure].cast(StringType())
        )
    
        return df_index_mailed