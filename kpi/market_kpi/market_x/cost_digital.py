from kpi.base_kpi.base_cost import BaseCost
from kpi.utilities import utils

class CostDigital(BaseCost):
    """
    Market_X Digital Cost module
    """
    def __init__(self, sc, client):
        super(CostDigital, self).__init__(sc, client)
        
        print 'Client Cost Digital module initiation (digital)'
    
    def market_kpi1(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 1 (digtial)'
        
    def market_kpi2(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 2 (digital)'