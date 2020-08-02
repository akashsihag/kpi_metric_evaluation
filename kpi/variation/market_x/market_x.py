from pyspark.sql.functions import udf, col, trim
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StringType

def market_x_seg(config, **dict):

    header_df = dict['table1'].filter(
        col('contact_stage_code') == 'ALC'
    )
    
    header_df = header_df.drop('contact_stage_code')
    print '1'
    header_df.cache()
    print header_df.count()
    print header_df.distinct().count()
    header_df.show()
    
    detail_df = dict['table2']
    
    # details control customers required
    detail_df = detail_df.withColumn("offer_rank_num", col("offer_rank_num").cast(DoubleType()))
     
    detail_df.cache()
    print '2'
    print detail_df.count()
    print detail_df.distinct().count()
    detail_df.show()
     
    detail_header_df = detail_df.join(header_df, 'prsn_code', 'left_outer')
    
    detail_header_df.cache()
    print '3'
    print detail_header_df.count()
    print detail_header_df.distinct().count()
    detail_header_df.show(truncate=False)
    
    volume_udf = udf(
        lambda col1, col2: 'baulcm' if (
            col1 >= 1 and col1 <= 8 and col2 != 'Y' and col2 is not None
        ) else 'lessloyal' if(
            col1 >= 1 and col1 <= 3 and col2 == 'Y'
        ) else 'baby' if(
            col1 >= 9 and col1<= 12 
        ) else 'npm' if(
            col1 >= 13 and col1 <= 20
        ) else 'extralcm' if(
            col1 >= 21 and col1 <= 24
        ) else 'babyextra' if(
            col1 >= 25 and  col1 <= 28
        ) else 'nfm' if(
            col1 >= 29 and col1 <=32
        ) else 'additional' if(
            col1 >= 33 and col1 <= 40
        ) else 'others'
    )

    slot_df = detail_header_df.withColumn(
        'slot_segment',
        (volume_udf(col('offer_rank_num'), col('less_loyal_flag')))
    )
    
    slot_df.cache()
    print '4'
    print slot_df.count()
    print slot_df.distinct().count()
    slot_df.filter(col('contact_stage_code') == 'RDM').show(200)
    
    slot_df = slot_df.drop('offer_rank_num')
    slot_df = slot_df.drop('less_loyal_flag')
    
    print 'count for slot_df'
    slot_df.cache()
    print slot_df.count()
    print slot_df.distinct().count()
    slot_df.show()
    
    card_lifestyle_seg_df = dict['card_lifestyle_seg'].filter(trim(col(config['identity_type_code'])) != '')
    slot_segment_df = slot_df.join(card_lifestyle_seg_df, config['identity_type_code'], 'left_outer')
    
    print 'count for slot_segment_df'
    slot_segment_df.cache()
    print slot_segment_df.count()
    print slot_segment_df.distinct().count()
    slot_segment_df.show()
    
    card_loyalty_seg_df = dict['card_loyalty_seg'].filter(trim(col(config['identity_type_code'])) != '')
    slot_segment_df = slot_segment_df.join(card_loyalty_seg_df, config['identity_type_code'], 'left_outer')
    
    print 'count for slot_segment_df'
    slot_segment_df.cache()
    print slot_segment_df.count()
    print slot_segment_df.distinct().count()
    slot_segment_df.show()
    
    card_pricesence_seg_df = dict['card_pricesence_seg'].filter(col(config['identity_type_code']) != '')
    slot_segment_df = slot_segment_df.join(card_pricesence_seg_df, config['identity_type_code'], 'left_outer')
    
    print 'count for slot_segment_df'
    slot_segment_df.cache()
    print slot_segment_df.count()
    print slot_segment_df.distinct().count()
    slot_segment_df.show()
    
    date_df = dict['date_dim']
    targ_date = date_df.filter(col("fis_week_id") == str(config['seg_week'])).filter(col('fis_day_of_week_num') == '7').select(col('date').cast(StringType())).collect()[0][0]
    
    # age
    card_dim_df = dict['card_dim'].withColumn(
        'age_1',
        F.floor(F.datediff(F.lit(targ_date),F.col('card_birth_date')))/365).withColumn(
        'Age',
        F.when(F.col('age_1') > 66 ,'67 eller mer').otherwise(
            F.when(F.col('age_1') > 55 ,'56-66').otherwise(
                F.when(F.col('age_1') > 45 ,'46-55').otherwise(
                    F.when(F.col('age_1') > 35 ,'36-45').otherwise(
                        F.when(F.col('age_1') > 25 ,'26-35').otherwise(
                            F.when(F.col('age_1') > 0,'0-25').otherwise(
                                'Uklassifiserte')
                        )
                    )
                )
            )
        )
    ).drop(F.col('age_1'))
    slot_segment_df = slot_segment_df.join(card_dim_df, config['identity_type_code'], 'left_outer')
    
    slot_segment_df.cache()
    print '6'
    print slot_segment_df.count()
    print slot_segment_df.distinct().count()
    slot_segment_df.show()
    
    # supplier_name
    print dict
    offer_dim_df = dict['offer_dim']
#     .select("offer_code", "supplier_name").distinct()
    
    
    # CHECK FOR NULL EMPTY OFFER_CODE and empty
    offer_dim_df = offer_dim_df.filter(trim(col("offer_code")) != "").groupBy('offer_code','supplier_name').agg(F.max('offer_discount_amt').alias("offer_discount_amt"), F.max('offer_amount').alias("offer_amount")).dropDuplicates(['offer_code'])
    
    # removed ACT after discussion with Sharang
    slot_segment_df = slot_segment_df.join(
        offer_dim_df,
        'offer_code',
        'left_outer'
    ).select(
        slot_segment_df["*"],
        offer_dim_df.supplier_name,
        offer_dim_df.offer_amount,
        F.when(
            (slot_segment_df.contact_stage_code.isin("ALC", "DLV", "EXP")),
            offer_dim_df.offer_discount_amt
        ).otherwise(
            slot_segment_df.offer_discount_amt
        ).alias("offer_discount_amt")
    ).drop(slot_segment_df.offer_discount_amt)
    
    slot_segment_df.cache()
    print '7'
    print slot_segment_df.count()
    print slot_segment_df.distinct().count()
    slot_segment_df.show()
    
    # private label and prod_hier_l20_code
#     prod_dim_df = dict['prod_dim'].withColumn('supplier_private_label', F.when(
#         F.upper(F.col('prod_desc')).like('%X-TRA%'),
#         F.lit('private')
#     ).when(
#         F.upper(F.col('prod_desc')).like('%NGLAMARK%'),
#         F.lit('private')
#     ).when(
#         F.upper(F.col('prod_desc')).like('%MARKET%'),
#         F.lit('private')
#     ).otherwise(F.lit('non-private')))
#     slot_segment_df = slot_segment_df.join(prod_dim_df, 'prod_code', 'left_outer')
    
    # banner_name
#     store_dim_df = dict['store_dim']
#     slot_segment_df = slot_segment_df.join(store_dim_df, 'store_code', 'left_outer')
    
    return slot_segment_df
