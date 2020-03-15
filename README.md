# %%
import datetime
import pytz
import pandas
import os
import pandas as pd
from google.cloud import bigquery
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = \
    "/Users/adamszpilewicz/PycharmProjects/soft_collection/002_soft_collection/schedulin/tests/python/dev-33-datalake-57f58776d81a.json"
client = bigtable.Client(project='dev-33-datalake', admin=True)
client_bq = bigquery.Client(project='dev-33-datalake')
