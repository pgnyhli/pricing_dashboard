#%%
import requests
import pandas as pd
import math
import time
import configparser
import snowflake
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import MySQLdb.cursors
import datetime as dt
import yaml
import os
import concurrent.futures
#%%
with open('config/configs.yaml','r') as file: 
    credentials = yaml.safe_load(file)

config = credentials['snowflake']

conn_sf = snowflake.connector.connect(
    user = config['UID'],
    password = config['PWD'],
    account = config['acct'],
    warehouse = config['warehouse']
)
cursor_sf = conn_sf.cursor()
# snowflake queries -> need distinct session id's
def execute_snowflake():
    q1 = """
    select * from FINANCEBI_DB.HLI.hl_weighted_avg_out
    """
    return(q1)

q1 = execute_snowflake()
weighted_avg_df = pd.read_sql(q1,conn_sf)
# convert df to list 
weighted_avg_df
# %%

def weighted_avg_excluding_self(group):
    # Adding a column for weighted cost
    group['WEIGHTED_COST'] = group['COST'] * group['COUNT_AUTH']
    # Calculating the sum of COUNT_AUTH and WEIGHTED_COST for normalization
    total_count_auth = group['COUNT_AUTH'].sum()
    total_weighted_cost = group['WEIGHTED_COST'].sum()
    
    # Applying the calculation for each row
    def calc_excluded(row):
        # Adjusting totals by excluding the current row's values
        adjusted_count = total_count_auth - row['COUNT_AUTH']
        adjusted_weighted_cost = total_weighted_cost - row['WEIGHTED_COST']
        # Avoid division by zero
        if adjusted_count == 0:
            return 0
        # Calculating the weighted average cost excluding the current provider
        return adjusted_weighted_cost / adjusted_count
    
    # Applying the calculation to each row in the group
    group['WEIGHTED_AVG_COST_EXCL_SELF'] = group.apply(calc_excluded, axis=1)
    return group

# Grouping by CBSA and applying the function
df = weighted_avg_df.groupby('CBSA').apply(weighted_avg_excluding_self)

# Dropping the intermediate 'WEIGHTED_COST' column as it's no longer needed
df.drop(columns=['WEIGHTED_COST'], inplace=True)

# Displaying the modified DataFrame
df
# %%
boston_ivf_df = df[(df['AUTHORIZING_PROVIDER'] == 'Boston IVF') & (df['CBSA'] == 'Boston-Cambridge-Newton, MA-NH')]
boston_ivf_df
# %%
