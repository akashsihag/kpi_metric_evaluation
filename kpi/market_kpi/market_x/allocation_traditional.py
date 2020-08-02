from kpi.base_kpi.base_allocation import BaseAllocation
from kpi.utilities import utils

class AllocationTraditional(BaseAllocation):
    """
    Market_X Traditional Allocation module
    """
    def __init__(self, sc, client):
        super(AllocationTraditional, self).__init__(sc, client)
        
        print 'Client Allocation module initiation (traditional)'
        self.base_allocation_df = self.df_dict(self.config_dict['data_variation'])
    
    def market_kpi1(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 1 (traditional)'
        
    def market_kpi2(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 2 (traditional)'