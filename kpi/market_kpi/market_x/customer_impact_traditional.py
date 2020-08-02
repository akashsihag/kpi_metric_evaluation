from kpi.base_kpi.base_customer_impact import BaseCustomerImpact
from kpi.utilities import utils

from pyspark.sql.functions import lower

class CustomerImpactTraditional(BaseCustomerImpact):
    """
    Customer Impact module
    """
    def __init__(self, sc, client):
        super(CustomerImpactTraditional, self).__init__(sc, client)
        
        print 'Client Customer Impact module initiation'
    
    def market_kpi1(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 1'
        
    def market_kpi2(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 2'