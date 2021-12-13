from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import os

# Loading the .env file from the project
load_dotenv()

# Setting all globals
TOKEN = os.environ.get("ENPHASE_TOKEN")
USER_ID = os.environ.get("ENPHASE_USER_ID")
SYSTEM_ID = os.environ.get("SYSTEM_ID")
SYSTEM_URL = f"https://api.enphaseenergy.com/api/v2/systems?key={TOKEN}&user_id={USER_ID}"
PRODUCTION_URL = f'https://api.enphaseenergy.com/api/v2/systems/{SYSTEM_ID}/stats'
DB_STRING = os.environ.get('DB_STRING')
DB_STRING_RAS = os.environ.get('DB_STRING_RAS')

# Establish database connnections
local_db = create_engine(DB_STRING)
ras_db = create_engine(DB_STRING_RAS)

start_data = pd.read_sql_table('production', local_db, index_col=['time'])
start_data.to_sql('production', ras_db, if_exists='append')

print('Sucessfully Migrated!')
