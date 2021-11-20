import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy import Column, DateTime, SmallInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Loading the .env file from the project
load_dotenv()

# Setting all globals
TOKEN = os.environ.get("ENPHASE_TOKEN")
USER_ID = os.environ.get("ENPHASE_USER_ID")
SYSTEM_ID = os.environ.get("SYSTEM_ID")
SYSTEM_URL = f"https://api.enphaseenergy.com/api/v2/systems?key={TOKEN}&user_id={USER_ID}"
PRODUCTION_URL = f'https://api.enphaseenergy.com/api/v2/systems/{SYSTEM_ID}/rgm_stats'
DB_STRING = os.environ.get('DB_STRING')


def get_production_data_from_previous_week():
    """Gather last week's worth of production data from ENPHASE"""
    data = pd.DataFrame(columns=['time', 'production'])
    end_date = datetime.today()
    start_date = datetime.today() - timedelta(days=7)
    payload = {'key': TOKEN, 'user_id': USER_ID,
               'start_at': start_date.timestamp(), 'end_at': end_date.timestamp()}
    response = requests.get(PRODUCTION_URL, payload)
    body = response.json()
    devices = body.get('total_devices')
    for m in range(devices):
        for interval in body['meter_intervals'][m]['intervals']:
            if interval['channel'] != 1:
                break
            data = data.append({'time': datetime.fromtimestamp(interval['end_at']),
                                'production': interval['curr_w']}, ignore_index=True)
    data.set_index(['time'], inplace=True)
    return data


def append_production_data():
    """Get production data, compare with current database table, and append new data"""
    production_data = get_production_data_from_previous_week()

    db = create_engine(DB_STRING)

    existing_data = pd.read_sql_table('production', db, index_col=['time'])
    new_data = production_data.drop(existing_data.index, errors='ignore', axis=0)
    new_data.to_sql('production', db, if_exists='append')


if __name__ == '__main__':
    append_production_data()
