#!/usr/bin/env python3

import os
import time
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine

pd.set_option('display.max_rows', None)
# Loading the .env file from the project
load_dotenv()

# Setting all globals
TOKEN = os.environ.get("ENPHASE_TOKEN")
USER_ID = os.environ.get("ENPHASE_USER_ID")
SYSTEM_ID = os.environ.get("SYSTEM_ID")
SYSTEM_URL = f"https://api.enphaseenergy.com/api/v2/systems?key={TOKEN}&user_id={USER_ID}"
PRODUCTION_URL = f'https://api.enphaseenergy.com/api/v2/systems/{SYSTEM_ID}/stats'
DB_STRING = os.environ.get('DB_STRING')


def get_production_data_from_select_two_days(start_at, end_at):
    """Gather two days worth of production data from ENPHASE"""
    data = pd.DataFrame(columns=['time', 'production'])

    payload = {'key': TOKEN, 'user_id': USER_ID,
               'start_at': start_at.timestamp(), 'end_at': end_at.timestamp()}
    response = requests.get(PRODUCTION_URL, payload)

    body = response.json()

    for interval in body['intervals']:
        data = data.append({'time': datetime.fromtimestamp(interval['end_at']),
                                'production': interval['enwh']}, ignore_index=True)
    data.set_index(['time'], inplace=True)

    return data


def append_production_data(data):
    """Get production data, compare with current database table, and append new data"""

    db = create_engine(DB_STRING)

    existing_data = pd.read_sql_table('production', db, index_col=['time'])
    new_data = data.drop(existing_data.index, errors='ignore', axis=0)
    new_data.to_sql('production', db, if_exists='append')


if __name__ == '__main__':
    start_date = datetime.strptime('2021-03-31', '%Y-%m-%d')

    while True:
        end_date = start_date + timedelta(days=1)
        data = get_production_data_from_select_two_days(start_date, end_date)
        append_production_data(data)
        print(f'{start_date.strftime("%Y-%m-%d")} uploaded.')
        start_date = end_date
        time.sleep(7)