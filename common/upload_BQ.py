import os
from google.cloud import bigquery
import warnings
import json
from google.api_core.exceptions import BadRequest

client = bigquery.Client(project='prod-01-datalake', location="EU")
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/adamszpilewicz/Documents/json/test-05-datalake-1ff4d862e16e.json"
table_id =  [ 'dl_layer3_cr.tmp_collateral_allocation', ]
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1
job_config.autodetect = False


#==============================================================================================================================
#create new table in BQ
#==============================================================================================================================
client = bigquery.Client(project='prod-01-datalake', location="EU")
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/adamszpilewicz/Documents/json/test-05-datalake-1ff4d862e16e.json"
table_id =  [ 'dl_layer3_cr.tmp_collateral_allocation', ]
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
# --field_delimiter=(printf '^') 
job_config.skip_leading_rows = 1
job_config.autodetect = False
job_config.schema=[
bigquery.SchemaField("obligor_id"                  ,"string"  , mode="nullable", description=""),
bigquery.SchemaField("gbv_per_account"             ,"float64" , mode="nullable", description=""),
bigquery.SchemaField("temenos_account_id"          ,"string"  , mode="nullable", description=""),
bigquery.SchemaField("product_id"                  ,"string"  , mode="nullable", description=""),
bigquery.SchemaField("gbv_total_obligor"           ,"float64" , mode="nullable", description=""),
bigquery.SchemaField("share_gbv_contract_in_total" ,"float64" , mode="nullable", description=""),
bigquery.SchemaField("collateral_type_id"          ,"string"  , mode="nullable", description=""),
    bigquery.SchemaField("value_of_mortgage"           ,"float64" , mode="nullable", description="value of AION mortgage on the property"),
    bigquery.SchemaField("value_of_property"           ,"float64" , mode="nullable", description="value of underlying property from the table dim_credit_collateral_valuation with the valuation_status='active'"),
bigquery.SchemaField("value_of_allocated_mortgage" ,"float64" , mode="nullable", description="value of the allocated collateral on the account_id level, as the same obligor can have more than one account_id; allocation key bases on GBV share of the particular account_id in the total GBV of all account_ids of the obligor"),
bigquery.SchemaField("product_variant_code"        ,"string"  , mode="nullable", description=""),
bigquery.SchemaField("T_24"                        ,"string"  , mode="nullable", description=""),
bigquery.SchemaField("occupied_by_owner"           ,"string"  , mode="nullable", description=""),
    bigquery.SchemaField("linked_credit_quality"       ,"string"  , mode="nullable", description=""),
bigquery.SchemaField("repayment_capacity"          ,"string"  , mode="nullable", description=""),

bigquery.SchemaField("is_eligible_collateral"      ,"string"  , mode="nullable", description=""),
bigquery.SchemaField("rwa_class_pre"               ,"string"  , mode="nullable", description=""),
bigquery.SchemaField("pk_snapshot_date"            ,"date"    , mode="required", description="")

]

import pandas as pd
allocated_results=pd.read_csv('collaterals.csv')



allocated_results.columns = [
  'una',
    'obligor_id',
    'gbv_per_account',
    'temenos_account_id',
    'product_id',
    'gbv_total_obligor',
    'share_gbv_contract_in_total',
    'collateral_type_id',
    'value_of_mortgage',
      'value_of_property',
    'value_of_allocated_mortgage',

'product_id_drop',
    'product_variant_code',
    'T_24',
    'occupied_by_owner',
      'linked_credit_quality',
    'repayment_capacity',
    'is_eligible_collateral',
    'rwa_class_pre'
    
    
]
allocated_results['pk_snapshot_date']='2019-12-01'

allocated_results=allocated_results.drop(['product_id_drop'], axis=1).to_csv('res.csv', index=False)
allocated_results=allocated_results.drop(['una'], axis=1).to_csv('res.csv', index=False)

# # In[11]:
# # from pprint import pprint

# # allocated_results.to_csv('resa.csv', index=False)

# # display(pd.read_csv("resa.csv"))
# # # In[12]:
# # allocated_results.head()

# # script_dir = os.path.dirname(__file__)  # Script directory
# # # filepath = './res.csv'
# # #print(pd.read_csv("res.csv"))
with open('resa.csv', "r+b") as source_file:
    job = client.load_table_from_file(source_file, 'dl_layer3_cr.tmp_collateral_allocation', job_config=job_config)
try:
    job.result()
except BadRequest as e:
    for e in job.errors:
        print('ERROR: {}'.format(e['message']))