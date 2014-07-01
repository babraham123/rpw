'''
DV Loops: Select, aggregate, and parse domains

by Bereket Abraham
'''

from reader import *
from processor import *
from writer import *
import pandas as pd
import numpy as np


source = {
    "type":"hdfs",
    "location":"/dv/domain_hourly_blocks/",
    "filter":{
        "days_ago":2
    }
}
r = createReader(source)
domain_seller_dvcode = r.read()

# dv_block_reason,site_domain,Imps_blocked,Imps,MediaCost,Clicks,Convs
domain_dvcode = df.groupby(by=['site_domain','dv_block_reason'], as_index=False).aggregate(np.sum).sort(columns=['Imps'], ascending=[False])
domain_total = df.groupby(by=['site_domain'], as_index=False).aggregate(np.sum) # dv_block_reason broken
domain_total = domain_total[[ 'site_domain','Imps_blocked','Imps','MediaCost','Clicks','Convs' ]]

h = domain_total.columns
for i in range(len(h)):
    h[i] = h[i] + '_total'
domain_total.columns = h

domain_dvcode = merge(domain_dvcode, domain_total, left_on='site_domain', right_on='site_domain_total', how='left')

create_fraud = {
    "column_name_1":"Imps",
    "column_name_2":"Imps_total",
    "column_name_new":"Fraud",
    "operation":"/"
}

rule1 = {
    "selectors": [
        {
            "column_name":"Imps_total",
            "comparator":">",
            "value":5000
        },
        {
            "column_name":"dv_block_reason",
            "comparator":"==",
            "value":1
        }
    ]
}

processor = DataProcessor(dv_feed)
df = processor.operate(df)
df = processor.select(df)


dest = {
    "type":"mail",
    "filename":"domains.csv",
    "recipients": [
        "babraham@appnexus.com"
    ],
    "sender":"babraham@appnexus.com",
    "subject":"New Dataframe",
    "body":"Here is a new csv file of data."
}

w = createWriter(dest)
result = w.write(df)
print result

##

