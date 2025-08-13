import snowflake.connector 
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("USER"),
    password=os.getenv("PASSWORD"),
    account=os.getenv("ACCOUNT"),
    warehouse=os.getenv("WAREHOUSE"),
    database=os.getenv("DATABASE"),
    schema=os.getenv("SCHEMA")
)

cur = conn.cursor()
sql = '''
select * from raw.raw_data where load_date > (select max(etl_load_timestamp) from tranformed_data.stg_raw__cricket_raw_data)
'''

def fetch_pandas_old():
    cur.execute(sql)
    rows = 0
    while True:
        dat = cur.fetchmany(50000)
        if not dat:
            break
        df = pd.DataFrame(dat, columns=[desc[0] for desc in cur.description])
        rows += df.shape[0]
    
    if rows == 0:
        raise ValueError("No new data in raw_data table in Snowflake table, exiting..")  # Raise an exception
    
    print(f"Good to go! New matches: {rows}")