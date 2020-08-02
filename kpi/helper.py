import sys, os
from pyspark.sql import HiveContext

from core_modules.utils import functions
from pyspark.sql.functions import col

from kpi.base_kpi.base_module import BaseModule
import kpi
from kpi.utilities import utils

class Helper(BaseModule):
    
    def __init__(self, sc, config, client):
        super(Helper, self).__init__(sc, client)
        self.config = config
        self.measures = list()
    
    def factory(self, module_name, targetting_mode):
        """
        factory method for creating objects based
        on class name
        """
        kpi_module = getattr(getattr(kpi.market_kpi, self.config_dict['market']), module_name + '_' + targetting_mode)
        module_class = getattr(kpi_module, module_name.replace("_", " ").title().replace(" ", "") + targetting_mode.capitalize())
        return module_class(self.sc, self.client)
    
    def update_config(self):
        client_module = getattr(getattr(kpi.variation, self.config_dict['market']), self.config_dict['config_variation'])
        config_variation = getattr(client_module, self.config_dict['config_variation'])
        config_variation(self.sqlContext, self.config_dict)
        
    def get_variation(self):
        client_module = getattr(getattr(kpi.variation, self.config_dict['market']), self.config_dict['data_variation'])
        variation = getattr(client_module, self.config_dict['data_variation'])
        df = variation(self.config_dict, **self.df_dict())
        self.df_dict(self.config_dict['data_variation'], df, overwrite = True)
    
    def final_table(self):
        final_df = self.df_dict('empty_df')
        
        for module_name in self.config['module_names']:
            if (self.config['module_names'][module_name] == 'Y'):
                try:
                    module_df = self.sqlContext.table(
                        self.config_dict['output_db'] +
                        '.' + self.config_dict['output_prefix'] + 
                        '_' + module_name + '_' + 
                        self.config_dict['output_suffix']
                    )
                    module_df.cache()
                    module_df.show()
                except Exception as e:
                    print 'Unable to read table : {}.{}_{}_{}'.format(
                        self.config_dict['output_db'], 
                        self.config_dict['output_prefix'],
                        module_name, self.config_dict['output_suffix']
                    )
                    print (e)
                    
                if utils.valid_df(module_df):
                    module_df.cache()
                    module_df.show()
                    
                    common = list(set(module_df.columns).intersection(final_df.columns))
                    union = list(set(module_df.columns).union(final_df.columns))
                    
                    if len(common) > 0:
                        measures = list(set(union) - set(common))
                        diff_grouping_cloumns = list(set(measures) - set(self.measures))
                        final_df = module_df.join(
                            final_df, common,
                            'outer'
                        ).fillna('total', diff_grouping_cloumns).fillna('0', self.measures)
                    else:
                        union_df = self.sqlContext.createDataFrame(
                            [[''] * len(union)],
                            union
                        ).filter(col(union[1]) != '')
                        
                        final_df = functions.union_multi_df(
                            union_df,
                            final_df,
                            module_df,
                            column_sequence_df = 1
                        )
                        final_df.cache()
                        final_df.show()
                        
                if utils.valid_df(final_df):
                    final_df.cache()
                    final_df.show()
                    
                    final_df.registerTempTable('final_df_table')
                    self.sqlContext.sql(
                        "drop table if exists " +
                        self.config_dict['output_db'] + 
                        "." + self.config_dict['output_prefix']  +
                        "_" + self.config_dict['final_table_name'] + 
                        "_" + self.config_dict['output_suffix']
                    )
                    self.sqlContext.sql(
                        "create table " +
                        self.config_dict['output_db'] + 
                        "." + self.config_dict['output_prefix']  +
                        "_" + self.config_dict['final_table_name'] +
                        "_" + self.config_dict['output_suffix'] +
                        " as select * from final_df_table"
                    )
                
#                     final_df.write.saveAsTable(
#                         self.output_db + '.' + self.output_prefix  + '_' + self.final_table_name + '_' + self.output_suffix,
#                         mode = self.write_mode
#                     )
        if(self.config_dict['flush_module_tables'] == 'Y'):
            self.flush_module_tables()
        return final_df
    
    def run_modules(self):
        
        # read the configuration from config.ini
        self.read_config(self.config)
        
        # update the configuration from market logic
#        self.update_config()
        
        # extract the  dataframes defined in extractor.json
        self.extract_df()
        
        # extract the client data defined in Base Module
        self.extract_client_df()
        
        # variation data from variation package, this will varry according to market
        self.get_variation()
        
        # iterate over each module to trigger kpis
        for module_name in self.config['module_names']:
            if (self.config['module_names'][module_name] == 'Y'):
                for i in range(len(self.config[module_name]['list'])):
                    groupings_comb = self.config[module_name]['list'][i][0]
                    measures_comb = self.config[module_name]['list'][i][1]

                    groupings = self.config[module_name][groupings_comb]
                    measures = self.config[module_name][measures_comb]
                    
                    self.measures.extend(measures)
                    
#                     module_to_run = module_name + '_' + self.config_dict['targetting_mode']
                    
                    # create module object by passing module name
                    obj = self.factory(module_name, self.config_dict['targetting_mode'])
                    
                    # this will trigger all the mesures with gropings passed
                    # trigger_kpi is defined in base module
                    module_df = obj.trigger_kpi(groupings, measures)
                    
                    print 'module df'
                    module_df.cache()
                    module_df.show()

                    module_df.registerTempTable('module_df_table')
                    self.sqlContext.sql(
                        "drop table if exists " + 
                        self.config_dict['output_db'] + 
                        '.' + self.config_dict['output_prefix'] + 
                        '_' + module_name + '_' + 
                        self.config_dict['output_suffix']
                    )
                    self.sqlContext.sql(
                        "create table " + 
                        self.config_dict['output_db'] +
                        '.' + self.config_dict['output_prefix'] +
                        '_' + module_name + '_' +
                        self.config_dict['output_suffix'] + 
                        " as select * from module_df_table"
                    )
                    
#                     if utils.valid_df(module_df):
#                         module_df.write.saveAsTable(
#                             self.output_db + '.' + self.output_prefix + '_' + module_name + '_' + self.output_suffix, 
#                             mode = 'overwrite'
#                         )
