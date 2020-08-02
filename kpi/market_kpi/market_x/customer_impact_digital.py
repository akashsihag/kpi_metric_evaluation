from kpi.base_kpi.base_customer_impact import BaseCustomerImpact
from kpi.utilities import utils

class CustomerImpactDigital(BaseCustomerImpact):
    """
    Market_X Digital CustomerImpact module
    """
    def __init__(self, sc, client):
        super(CustomerImpactDigital, self).__init__(sc, client)
        
        print 'Client CustomerImpact module initiation (digital)'
    
    def market_kpi1(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 1 (digtial)'
        
    def market_kpi2(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 2 (digital)'