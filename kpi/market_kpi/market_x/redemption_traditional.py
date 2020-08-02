from kpi.base_kpi.base_redemption import BaseRedemption
from kpi.utilities import utils

class RedemptionTraditional(BaseRedemption):
    """
    Market_X Redemption module
    """
    def __init__(self, sc, client):
        super(RedemptionTraditional, self).__init__(sc, client)
        
        print 'Client Redemption module initiation'
    
    def market_kpi1(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 1'
        
    def market_kpi2(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 2'