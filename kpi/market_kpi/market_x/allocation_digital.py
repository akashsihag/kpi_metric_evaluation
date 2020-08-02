from kpi.base_kpi.base_allocation import BaseAllocation
from kpi.utilities import utils

class AllocationDigital(BaseAllocation):
    """
    Market_X Digital Allocation module
    """
    def __init__(self, sc, client):
        super(AllocationDigital, self).__init__(sc, client)
        
        print 'Client Allocation module initiation (digital)'
    
    def market_kpi1(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 1 (digtial)'
        
    def market_kpi2(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 2 (digital)'