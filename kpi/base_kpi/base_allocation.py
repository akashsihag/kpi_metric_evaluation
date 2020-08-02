from kpi.utilities import utils
from kpi.base_kpi.base_module import BaseModule

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lower, concat, col, lit, trim

class BaseAllocation(BaseModule):
    """
    allocation module
    """
    def __init__(self, sc, client):
        super(BaseAllocation, self).__init__(sc, client)
        
    # The average value of coupons received by household
    def coupon_value_per_customer(self, grouping_set, column_set, measure):
        df = self.base_allocation_df.filter(
            col('contact_stage_code') == 'EXP'
        )
        
        coupon_value_per_customer_df = utils.distinct_average(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            'offer_discount_amt',
            self.config_dict['identity_type_code']
        )
        return coupon_value_per_customer_df
    
    # No. of active customers considered for targeting based on targeting week and active flag in card_dim_c
    def active_customers(self, grouping_set, column_set, measure):
        df = self.base_allocation_df.filter(
            col('contact_stage_code') == 'ALC'
        )
        
        df = df.filter(
            (trim(col('loyalty_level')) != 'GO') &
            (trim(col('loyalty_level')) != 'LP')
        )
        
        df = df.filter(col('loyalty_level').isNotNull())
        
        active_customers_df = utils.distinct_count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return active_customers_df
        
    # Percentage of active customers
    def active_customers_percentage(self, grouping_set, column_set, measure):
        df = self.base_allocation_df.filter(
            col('contact_stage_code') == 'ALC'
        )
        
        total_customers = df.distinct().count()
        
        df = df.filter(
            (trim(col('loyalty_level')) != 'GO') &
            (trim(col('loyalty_level')) != 'LP')
        )
        
        df = df.filter(col('loyalty_level').isNotNull())
        
        active_customers_percentage_df = utils.percentage(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code'],
            total_customers
        )
        return active_customers_percentage_df
    
    # No. of active customers considered for targeting in top 2 shabits. PR and VL only
    def active_loyal_customer(self, grouping_set, column_set, measure):
        df = self.base_allocation_df.filter(
            col('contact_stage_code') == 'ALC'
        )
        
        df = df.filter(
            (trim(col('loyalty_level')) == 'PR') |
            (trim(col('loyalty_level')) == 'VL')
        )
        
        df = df.filter(col('loyalty_level').isNotNull())
        
        active_loyal_customer_df = utils.distinct_count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return active_loyal_customer_df
        
#     # Contactable Households
#     def contactable_households(self, grouping_set, column_set, measure):
#         df = self.base_allocation_df.filter(
#             self.base_allocation_df.contact_stage_code == 'DLV'
#         )
        
#         contactable_households_df = utils.distinct_count(
#             self.sqlContext,
#             df,
#             grouping_set,
#             column_set,
#             measure,
#             self.config_dict['identity_type_code']
#         )
#         return contactable_households_df
     
    # The total value of all coupons received, i.e. the total that households could redeem
    def coupon_value(self, grouping_set, column_set, measure):
        df = self.base_allocation_df.filter(
            col('contact_stage_code') == 'EXP'
        )

        coupon_value_df = utils.sum(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            'offer_discount_amt'
        )
        return coupon_value_df
        
    # The number of coupons allocated
    def coupons_allocated(self, grouping_set, column_set, measure):
        df = self.base_allocation_df.filter(
            col('contact_stage_code') == 'DLV'
        )

        coupon_allocated_df = utils.count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            'offer_code'
        )
        return coupon_allocated_df
        
    # The number of coupons delivered
    def coupons_exposed(self, grouping_set, column_set, measure):
        df = self.base_allocation_df.filter(
            col('contact_stage_code') == 'EXP'
        )
        
        coupon_mailed_df = utils.count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            'offer_code'
        )
        return coupon_mailed_df
    
    # Exposure stage for paper - sas data file for digital
    def exposed_households(self, grouping_set, column_set, measure):
        df = self.base_allocation_df.filter(
            col('contact_stage_code') == 'EXP'
        )
        
        exposed_households_df = utils.distinct_count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return exposed_households_df
      
    # The number of households mailed
    def mailed_households(self, grouping_set, column_set, measure):
        df = self.base_allocation_df.filter(
            col('contact_stage_code') == 'EXP'
        )
        
        mailed_households_df = utils.distinct_count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return mailed_households_df
    
    # The number of allocated ho-useholds
    def household_allocated(self, grouping_set, column_set, measure):
        df = self.base_allocation_df.filter(
            col('contact_stage_code') == 'DLV'
        )
        
        household_allocated_df = utils.distinct_count(
            self.sqlContext,
            df,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return household_allocated_df