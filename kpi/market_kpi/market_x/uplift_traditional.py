import matplotlib
matplotlib.use('PDF')
from kpi.base_kpi.base_uplift import BaseUplift
from kpi.utilities import utils

from pyspark.sql.functions import lower

class UpliftTraditional(BaseUplift):
    """
    Uplift module
    """
    def __init__(self, sc, client):
        super(UpliftTraditional, self).__init__(sc, client)
        
        print 'Client Uplift module initiation'
    
    def market_kpi1(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 1'
        
    def market_kpi2(self, grouping_set, column_set, measure):
        print 'This is a market specific definition 2'