#%%
import pandas as pd

import snowflake
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import datetime as dt
import yaml
from sqlalchemy import create_engine


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

df_2 = weighted_avg_df[['AUTHORIZING_PROVIDER']]
# print(df_2)
# %%

db = "FINANCEBI_DB"
schema = "HLI"
tb = "TEST_TABLE"
query = """TRUNCATE TABLE FINANCEBI_DB.HLI.TEST_TABLE"""

# Execute queries
# cursor_sf.execute(query) 
# write_pandas(conn_sf, df=df_2, database=db, schema=schema, table_name=tb)
# conn_sf.close()


# %%
#MySQL test

# MySQL test
with open('config/configs.yaml', 'r') as file:
    mysqlcredentials = yaml.safe_load(file)['MySQL']

engine = create_engine(f"mysql+mysqlconnector://{mysqlcredentials['user']}:{mysqlcredentials['password']}@{mysqlcredentials['host']}/{mysqlcredentials['database']}")

df_3 = pd.DataFrame(df_2)
print(df_3)

df_3.to_sql(name='HLI_TEST_TABLE', con=engine, if_exists='replace', index=False)
print("finished")