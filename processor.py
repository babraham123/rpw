import json, datetime
import pandas as pd
import numpy as np

class DataProcessor:
    ''' Process pandas dataframe according to operator and selector rules defined in the config file

    example config::
    "operators": [
                {
                    "column_name_1":"Imps_blocked",
                    "column_name_2":"Imps",
                    "column_name_new":"Fraud",
                    "operation":"/"
                }
            ],
    "selectors": [
                {
                    "column_name":"dv_block_reason",
                    "comparator":"==",
                    "value":1
                }
            ]
    '''

    def __init__(self, config):
        self.config = config

    def operate(self, df):
        #headers = df.columns.values.tolist()
        if not df or 'operators' not in self.config:
            return df

        for operator in self.config['operators']:
            df = self.operate_single(df, operator)

        return df

    def operate_single(self, df, operator):
        if operator['operation'] == '+':
            def op_func(row):
                return row[operator['column_name_1']] + row[operator['column_name_2']]
        elif operator['operation'] == '-':
            def op_func(row):
                return row[operator['column_name_1']] - row[operator['column_name_2']]
        elif operator['operation'] == '*':
            def op_func(row):
                return row[operator['column_name_1']] * row[operator['column_name_2']]
        elif operator['operation'] == '/':
            def op_func(row):
                if row[operator['column_name_2']] == 0:
                    return np.nan
                return row[operator['column_name_1']] / row[operator['column_name_2']]
        elif operator['operation'] == '(-)':
            def op_func(row):
                return -row[operator['column_name_1']]
        elif operator['operation'] == 'str':
            def op_func(row):
                return str(row[operator['column_name_1']])
        elif operator['operation'] == 'int':
            def op_func(row):
                return int(row[operator['column_name_1']])
        else:
            raise Exception("Unknown rule")

        df[ operator['column_name_new']] = df.apply( op_func, axis=1)
        return df


    
    def select(self, df):
        #headers = df.columns.values.tolist()
        if not df or 'selectors' not in self.config:
            return df

        for selector in self.config['selectors']:
            df = self.select_single(df, selector)
                
        return df

    def select_single(self, df, selector):
        if selector['comparator'] == '==':
            df = df[ df[ selector['column_name'] ] == selector['value'] ]
        elif selector['comparator'] == '>':
            df = df[df[ selector['column_name'] ] > selector['value']]
        elif selector['comparator'] == '<':
            df = df[df[ selector['column_name'] ] < selector['value']]
        elif selector['comparator'] == '>=':
            df = df[df[ selector['column_name'] ] >= selector['value']]
        elif selector['comparator'] == '<=':
            df = df[df[ selector['column_name'] ] <= selector['value']]
        elif selector['comparator'] == '!=':
            df = df[df[ selector['column_name'] ] != selector['value']]
        elif selector['comparator'] == 'null':
            df = df[ pd.isnull(df[ selector['column_name'] ]) ]
        elif selector['comparator'] == 'not null':
            df = df[ pd.isnull(df[ selector['column_name'] ])==False ]
        else:
            raise Exception("Unknown rule")
                
        return df


