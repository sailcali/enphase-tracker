#!/usr/bin/enphase-tracker/venv/bin/python3

import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy import Column, DateTime, SmallInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# For debugging - view all rows in terminal
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


def get_production_data_from_select_day(start_at):
    """Gather one day worth of production data from ENPHASE"""
    data = pd.DataFrame(columns=['time', 'production'])

    # Request data from API
    payload = {'key': TOKEN, 'user_id': USER_ID,
               'start_at': start_at}
    response = requests.get(PRODUCTION_URL, payload)
    body = response.json()

    # Load data into the DataFrame
    for interval in body['intervals']:
        data = data.append({'time': datetime.fromtimestamp(interval['end_at']),
                                'production': interval['enwh']}, ignore_index=True)
    data.set_index(['time'], inplace=True)

    return data


def append_production_data(data):
    """Append new data to production table"""
    db = create_engine(DB_STRING)

    # existing_data = pd.read_sql_table('production', db, index_col=['time'])
    # new_data = data.drop(existing_data.index, errors='ignore', axis=0)
    data.to_sql('enphase_production', db, if_exists='append')


if __name__ == '__main__':

    # Establish connection only to determine when the last entry was
    db = create_engine(DB_STRING)
    with db.connect() as con:
        sql = 'SELECT date(time) FROM enphase_production ORDER BY time DESC LIMIT 1;'
        result = con.execute(sql)
        last_date = result.fetchone()[0]
    last_date = last_date + timedelta(days=1)
    my_time = datetime.min.time()
    last_datetime = datetime.combine(last_date, my_time)
    start_date = last_datetime.timestamp()  # start date will be the day after most recent in database

    # Get production data for current date, append to table, and then move to next day
    while True:
        data = get_production_data_from_select_day(start_date)
        append_production_data(data)
        start_date = start_date + 86400
        if start_date > datetime.now().timestamp():
            break
