#!/usr/bin/enphase-tracker/venv/bin/python3

import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import requests
from discordwebhook import Discord

from accessInstance import AccessInstance

# For debugging - view all rows in terminal
pd.set_option('display.max_rows', None)

# Loading the .env file from the project
load_dotenv()

# Setting all globals
API_KEY = os.environ.get("ENPHASE_KEY")
SYSTEM_ID = os.environ.get("SYSTEM_ID")
CHANNEL_URL = os.getenv('ENPHASE_CHANNEL_URL')

SYSTEM_URL = f"api.enphaseenergy.com/api/v4/systems/{SYSTEM_ID}/summary?key={API_KEY}"
PRODUCTION_URL = f'https://api.enphaseenergy.com/api/v4/systems/{SYSTEM_ID}/telemetry/production_micro'


def get_production_data_from_select_day(start_at):
    """Gather one day worth of production data from ENPHASE"""
    data = pd.DataFrame(columns=['time', 'production'])

    # Request data from API
    header = {'Authorization': "Bearer " + ACCESS_DATA.access_token}
    params = {"key": API_KEY, "start_at": start_at}
    response = requests.get(PRODUCTION_URL, params=params, headers=header)
    body = response.json()
    # Load data into the DataFrame
    try:
        for interval in body['intervals']:
            if interval['enwh'] != 0:
                data = data.append({'time': datetime.fromtimestamp(interval['end_at']),
                                        'production': interval['enwh']}, ignore_index=True)
        data.set_index(['time'], inplace=True)
    except KeyError:
        discord = Discord(url=CHANNEL_URL)
        discord.post(content=f"No enphase data to record!\nError: {body}%")
        quit()
    return data


if __name__ == '__main__':
    try:
        # Create Access instance which manages the OAUTH2.0 keys and handles the db connection
        ACCESS_DATA = AccessInstance()

        # Record the last day of data in the database
        with ACCESS_DATA.db.connect() as con:
            sql = 'SELECT date(time) FROM enphase_production ORDER BY time DESC LIMIT 1;'
            result = con.execute(sql)
            last_date = result.fetchone()[0]
        last_date = last_date + timedelta(days=1)
        my_time = datetime.min.time()
        last_datetime = datetime.combine(last_date, my_time)
        start_date = int(last_datetime.timestamp())  # start date will be the day after most recent in database
        # Get production data for current date, append to table, and then move to next day
        while True:
            data = get_production_data_from_select_day(start_date)
            data.to_sql('enphase_production', ACCESS_DATA.db, if_exists='append')
            start_date = start_date + 86400
            if start_date > datetime.now().timestamp():
                break
    except Exception as error:
        discord = Discord(url=CHANNEL_URL)
        discord.post(content=f"Enphase data was either partially recorded or not recorded at all!\nError: {error}")
