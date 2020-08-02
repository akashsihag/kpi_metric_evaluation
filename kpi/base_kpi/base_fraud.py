from kpi.utilities import utils
from kpi.base_kpi.base_module import BaseModule

from pyspark.sql.functions import lower, col, trim
from core_modules.utils import functions

from pyspark.sql.types import DoubleType, StringType
import pyspark.sql.functions as F

class BaseFraud(BaseModule):
    """
    fraud module
    """
    def __init__(self, sc, client):
        super(BaseFraud, self).__init__(sc, client)
            
        self.df = self.df_dict(self.config_dict['data_variation'])
        
        self.df_alc = self.df.filter(col('contact_stage_code') == 'ALC').select(self.config_dict['identity_type_code'], 'event_control_flag').distinct()
        
        self.df_redeem = self.df.filter(col('contact_stage_code') == 'RDM').drop('event_control_flag')
        self.df_redeem = self.df_redeem.join(self.df_alc, [self.config_dict['identity_type_code']], 'left_outer')
        
        self.df_multi_app = self.df_dict('empty_df')
        self.df_multi_dm = self.df_dict('empty_df')
        self.df_multi_appdm = self.df_dict('empty_df')
     
    # at overall level after discussion with Sharang
    # QA with Sharan and Archit
    def bad_redeemers(self, grouping_set, column_set, measure):
        non_cc_df_redeem = self.df_redeem.filter(
            ~((trim(col(self.config_dict['identity_type_code'])) == '') |
            (col(self.config_dict['identity_type_code']).isNull()) |
            (col(self.config_dict['identity_type_code']) == 'null'))
        )
        df_rdm_prsn_off = non_cc_df_redeem.select(
            self.config_dict['identity_type_code'],
            'offer_code'
        ).dropDuplicates()
        
        df_alc_prsn_off = self.df.filter(
            self.df.contact_stage_code=='EXP'
        ).select(
            self.config_dict['identity_type_code'], 'offer_code'
        ).dropDuplicates()
    
        df_rdm_other_alc_off = df_rdm_prsn_off.subtract(df_alc_prsn_off)
    
        dup_df = non_cc_df_redeem.groupBy(
                [self.config_dict['identity_type_code'],'offer_code']
            ).count().filter('count > 1').drop('count')
        
        final = dup_df.union(df_rdm_other_alc_off)

        final_df = self.sqlContext.createDataFrame(
            [["overall"]],
            ["grouping_level"]
        )
        
        bad_red = final.select(self.config_dict['identity_type_code']).distinct().count()
        
        final_df = final_df.withColumn(
            measure,
            F.lit(bad_red).cast(StringType())
        )
        
        for col_name in column_set:
            final_df = final_df.withColumn(col_name, F.lit('total'))
            
        final_df = final_df.withColumn('grouping_level', F.lit('overall'))
        return final_df
    
    # QA with Sharan and Archit
    def control_redeemers(self, grouping_set, column_set, measure):
        df_redem_control_details = self.df_redeem.filter(
            (self.df_redeem.event_control_flag == 'Y')
        )
        
        df_final = utils.distinct_count(
            self.sqlContext,
            df_redem_control_details,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return df_final

    # QA with Sharan and Archit
    def non_cc_malredemptions(self, grouping_set, column_set, measure):
        df_noncc = self.df_redeem.filter(
            (trim(col(self.config_dict['identity_type_code'])) == '') |
            (col(self.config_dict['identity_type_code']).isNull()) |
            (col(self.config_dict['identity_type_code']) == 'null')
        )
        
        final_df = self.sqlContext.createDataFrame(
            [["overall"]],
            ["grouping_level"]
        )
        
        final_df = final_df.withColumn(
            measure,
            F.lit(df_noncc.count()).cast(StringType())
        )
        
        for col_name in column_set:
            final_df = final_df.withColumn(col_name, F.lit('total'))
            
        final_df = final_df.withColumn('grouping_level', F.lit('overall'))
        return final_df
    
    # QA with Sharan and Archit
    def control_participation(self, grouping_set, column_set, measure):
        df_control = self.df.where(
            (col('contact_stage_code') == 'ALC') &
            (col('event_control_flag') == 'Y')
        ).drop('prod_code')
#         .select(self.config_dict['identity_type_code'])
        
        if not df_control.head(1):
            column_set.append(measure)
            group_set = column_set + ['grouping_level']
            df_final = sqlContext.createDataFrame(
                [[''] * len(group_set)],
                group_set
            )
            column_set.remove(measure)
        else:
            df_trans = df_control.join(
                self.df_dict('dur_period').select(self.config_dict['identity_type_code'], 'prod_code'),
                self.config_dict['identity_type_code']
            )
            
            df_prod = df_trans.join(
                self.df_dict('table3').select('prod_code'),
                ['prod_code']
            )
            
            df_final = utils.distinct_count(
                self.sqlContext,
                df_prod,
                grouping_set,
                column_set,
                measure, 
                self.config_dict['identity_type_code']
            )
        return df_final
        
    # archit
    def mis_redemptions(self, grouping_set, column_set, measure):
        df_subtract = self.df_redeem.select(
            'prod_code',
            'offer_code'
        ).dropDuplicates().subtract(
            self.df_dict('table3').select(
                'prod_code',
                'offer_code'
            ).dropDuplicates()
        )
        
        df_subtract_prsn = df_subtract.join(self.df_redeem, ['prod_code','offer_code'])
        
        # since f.count() don't consider null customers
        df_subtract_prsn = df_subtract_prsn.fillna('null', ['prsn_code'])
        
        df_final = utils.count(
            self.sqlContext,
            df_subtract_prsn,
            grouping_set,
            column_set,
            measure,
            self.config_dict['identity_type_code']
        )
        return df_final
    
    # QA with Sharan and Archit
    def app_multi_redeemers(self, grouping_set, column_set, measure):
        if not self.df_multi_app.head(1):
            df_digital = self.df_redeem.filter(
                lower(col('channel_code')).like("%digital%")
            )
            
            dup_df = df_digital.groupby(
                [self.config_dict['identity_type_code'], 'offer_code']
            ).count().filter('count > 1')
            
            df_inter = dup_df.join(
                df_digital,
                [self.config_dict['identity_type_code'], 'offer_code'],
                'left_outer'
            )
            
            df_final = utils.distinct_count(
                self.sqlContext,
                df_inter,
                grouping_set,
                column_set,
                measure,
                self.config_dict['identity_type_code']
            )
            
            self.df_multi_app = df_final
        else:
            df_final = self.df_multi_app
        return df_final
    
    # archit
    # QA with Sharan and Archit
    def dm_multi_redeemers(self, grouping_set, column_set, measure):
        if not self.df_multi_dm.head(1):
            df_paper = self.df_redeem.filter(
                lower(col('channel_code')).like("%paper%")
            )
            
            dup_df = df_paper.groupby(
                [self.config_dict['identity_type_code'], 'offer_code']
            ).count().filter('count > 1')
            
            df_inter = dup_df.join(
                df_paper,
                [self.config_dict['identity_type_code'],'offer_code'],
                'left_outer'
            )
            
            df_final = utils.distinct_count(
                self.sqlContext,
                df_inter,
                grouping_set,
                column_set,
                measure,
                self.config_dict['identity_type_code']
            )
            
            self.df_multi_dm = df_final
        else:
            df_final = self.df_multi_dm
        return df_final
    
    # archit
    # QA with Sharan and Archit
    def app_dm_multi_redeemers(self,grouping_set,column_set,measure):
        if not self.df_multi_appdm.head(1):
            df_paper = self.df_redeem.filter(
                lower(col('channel_code')).like("%paper%")
            )
            
            df_digital = self.df_redeem.filter(
                lower(col('channel_code')).like("%digital%")
            )
            
            dup_df_paper = df_paper.select(
                self.config_dict['identity_type_code'],
                'offer_code'
            ).dropDuplicates()
            
            dup_df_digital = df_digital.select(
                self.config_dict['identity_type_code'],
                'offer_code'
            ).dropDuplicates()
            
#             df_union = functions.union_multi_df(
#                 dup_df_paper,
#                 dup_df_digital,
#                 column_sequence_df = 1
#             )
            
            dup_df = dup_df_paper.intersect(dup_df_digital)
            
#             dup_df = df_union.groupBy(
#                 [self.identity_type_code,'offer_code']
#             ).count().filter('count > 1')
            
            df_inter = dup_df.join(
                self.df_redeem,
                [self.config_dict['identity_type_code'], 'offer_code'],
                'left_outer'
            )
            
            df_final = utils.distinct_count(
                self.sqlContext,
                df_inter,
                grouping_set,
                column_set,
                measure,
                self.config_dict['identity_type_code']
            )
            
            self.df_multi_appdm = df_final
        else:
            df_final = self.df_multi_appdm
        return df_final
    
    
    # at overall level after discussion with Sharang
    # QA with Sharan and Archit
    def mal_redemptions(self, grouping_set, column_set, measure):
        non_cc_df_redeem = self.df_redeem.filter(
            ~((trim(col(self.config_dict['identity_type_code'])) == '') |
            (col(self.config_dict['identity_type_code']).isNull()) |
            (col(self.config_dict['identity_type_code']) == 'null'))
        )
        df_rdm_prsn_off = non_cc_df_redeem.select(
            self.config_dict['identity_type_code'],
            'offer_code'
        ).dropDuplicates()
        
        df_alc_prsn_off = self.df.filter(
            self.df.contact_stage_code=='EXP'
        ).select(
            self.config_dict['identity_type_code'], 'offer_code'
        ).dropDuplicates()
        
        df_rdm_other_alc_off = df_rdm_prsn_off.subtract(df_alc_prsn_off)
        
        dup_df = non_cc_df_redeem.groupBy(
                [self.config_dict['identity_type_code'],'offer_code']
            ).count().filter('count > 1').drop('count')
        
        final = dup_df.union(df_rdm_other_alc_off)
        
        final_df = self.sqlContext.createDataFrame(
            [["overall"]],
            ["grouping_level"]
        )
        
        final_df = final_df.withColumn(
            measure,
            F.lit(final.count()).cast(StringType())
        )
        
        for col_name in column_set:
            final_df = final_df.withColumn(col_name, F.lit('total'))
            
        final_df = final_df.withColumn('grouping_level', F.lit('overall'))

        return final_df