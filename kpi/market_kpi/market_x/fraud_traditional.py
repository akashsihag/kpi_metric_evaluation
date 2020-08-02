from kpi.base_kpi.base_fraud import BaseFraud
from kpi.utilities import utils

from pyspark.sql.functions import lower

class FraudTraditional(BaseFraud):
    """
    Market_X Fraud module
    """
    def __init__(self, sc, client):
        super(FraudTraditional, self).__init__(sc, client)
        
        print 'Client Fraud module initiation'
 
    def market_kpi1(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 1'
        
    def market_kpi2(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 2'