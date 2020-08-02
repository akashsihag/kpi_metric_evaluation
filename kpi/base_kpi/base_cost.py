from kpi.utilities import utils
from kpi.base_kpi.base_module import BaseModule
from pyspark.sql.functions import col

from pyspark.sql.types import DoubleType, StringType

class BaseCost(BaseModule):
    """
    cost module
    """
    def __init__(self, sc, client):
        super(BaseCost, self).__init__(sc, client)
        
        self.redeemed_df = self.df_dict('empty_df')
        self.targeted_df = self.df_dict('empty_df')
        
    def redemption_cost(self, grouping_set, column_set, measure):
        if not self.redeemed_df.head(1):
            df_redem_details = self.base_cost_df.filter(
                col('contact_stage_code') =='RDM'
            )
            
            df_final = utils.sum(
                self.sqlContext,
                df_redem_details,
                grouping_set,
                column_set,
                measure,
                'offer_discount_amt'
            )
            
            self.redeemed_df = df_final
        else:
            df_final = self.redeemed_df
        return df_final
  
    def targeting_cost(self, grouping_set, column_set, measure):
        if not self.targeted_df.head(1):
            df_targeted = self.base_cost_df.filter(
                ((col('channel_code').like('%DIGITAL%')) 
                    & (col('contact_stage_code') == 'ACT'))
                | (~(col('channel_code').like('%DIGITAL%'))
                   &(col('contact_stage_code') == 'DLV'))
            )
            
            df_final = utils.sum(
                self.sqlContext,
                df_targeted,
                grouping_set,
                column_set,
                measure,
                'offer_amount'
            )
            
            self.targeted_df = df_final
        else:
            df_final = self.self.targeted_df
        return df_final

    def production_cost(self, grouping_set, column_set, measure):
        df_targeted = self.base_cost_df.filter(
            ((col('channel_code').like('%DIGITAL%'))
             & (col('contact_stage_code') == 'ACT'))
            | (~(col('channel_code').like('%DIGITAL%'))
               & (col('contact_stage_code') == 'DLV'))
        )
        
        df_final = utils.distinct_average(
            self.sqlContext, 
            df_targeted,
            grouping_set,
            column_set,
            measure,
            'offer_amount',
            self.config_dict['identity_type_code']
        )
        return df_final

    def campaign_cost(self, grouping_set, column_set, measure):
        if not self.redeemed_df.head(1):
            df_final = self.redemption_cost(
                grouping_set,
                column_set,
                'redemption_cost'
            )
            
            self.redeemed_df = df_final
        else:
            df_final = self.redeemed_df
        if not self.targeted_df.head(1):
            df_final1 = self.targeting_cost(
                grouping_set,
                column_set,
                'targeting_cost'
            )
            
            self.targeted_df = df_final1
        else:
            df_final1 = self.targeted_df
        group_set = column_set + ['grouping_level']
        df_return1 = df_final.join(df_final1, group_set)
        df_return1 = df_return1.fillna('0', ['targeting_cost', 'redemption_cost'])
        df_return1 = df_return1.withColumn(
            'targeting_cost',
            df_return1['targeting_cost'].cast(DoubleType())
        )
        
        df_return1 = df_return1.withColumn(
            'redemption_cost',
            df_return1['redemption_cost'].cast(DoubleType())
        )
        
        df_campaign_cost0 = df_return1.withColumn('campaign_cost',
                                                  df_return1.redemption_cost + df_return1.targeting_cost
                                                 )
        
        df_campaign_cost1 = df_campaign_cost0.drop('targeting_cost')
        df_campaign_cost2 = df_campaign_cost1.drop('redemption_cost')
        
        df_campaign_cost3 = df_campaign_cost2.withColumn(
            measure,
            df_campaign_cost2[measure].cast(StringType())
        )
        
        return df_campaign_cost3
