from kpi.base_kpi.base_fraud import BaseFraud
from kpi.utilities import utils

class FraudDigital(BaseFraud):
    """
    Market_X Digital Fraud module
    """
    def __init__(self, sc, client):
        super(FraudDigital, self).__init__(sc, client)
        
        print 'Client Fraud module initiation (digital)'
    
    def market_kpi1(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 1 (digtial)'
        
    def market_kpi2(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 2 (digital)'