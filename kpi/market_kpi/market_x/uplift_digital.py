from kpi.base_kpi.base_uplift import BaseUplift
from kpi.utilities import utils

class UpliftDigital(BaseUplift):
    """
    Market_X Digital Uplift module
    """
    def __init__(self, sc, client):
        super(UpliftDigital, self).__init__(sc, client)
        
        print 'Client Uplift module initiation (digital)'
    
    def market_kpi1(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 1 (digtial)'
        
    def market_kpi2(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 2 (digital)'