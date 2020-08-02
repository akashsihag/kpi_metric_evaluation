from kpi.base_kpi.base_redemption import BaseRedemption
from kpi.utilities import utils

class RedemptionDigital(BaseRedemption):
    """
    Market_X Digital Redemption module
    """
    def __init__(self, sc, client):
        super(RedemptionDigital, self).__init__(sc, client)
        
        print 'Client Redemption module initiation (digital)'
    
    def market_kpi1(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 1 (digtial)'
        
    def market_kpi2(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 2 (digital)'