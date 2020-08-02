from kpi.base_kpi.base_cost import BaseCost
from kpi.utilities import utils

class CostTraditional(BaseCost):
    """
    Market_X Cost module
    """
    def __init__(self, sc, client):
        super(CostTraditional, self).__init__(sc, client)
        
        print 'Client Cost module initiation'
        self.base_cost_df = self.df_dict(self.config_dict['data_variation'])
        
    def market_kpi1(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 1'
        
    def market_kpi2(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 2'