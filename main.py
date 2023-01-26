#!/usr/bin/enphase-tracker/venv/bin/python3

import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import requests
from discordwebhook import Discord

from accessInstance import AccessInstance

# Loading the .env file from the project
load_dotenv()

# Setting all globals
API_KEY = os.environ.get("ENPHASE_KEY")
SYSTEM_ID = os.environ.get("SYSTEM_ID")
CHANNEL_URL = os.getenv('ENPHASE_CHANNEL_URL')
SERVER_IP = os.environ.get("SERVER_IP")

SYSTEM_URL = f"api.enphaseenergy.com/api/v4/systems/{SYSTEM_ID}/summary?key={API_KEY}"
PRODUCTION_URL = f'https://api.enphaseenergy.com/api/v4/systems/{SYSTEM_ID}/telemetry/production_micro'


def get_production_data_from_select_day(current_date_timestamp):
    """Gather one day worth of production data from ENPHASE"""
    data = []
    # Request data from API
    header = {'Authorization': "Bearer " + ACCESS_DATA.access_token}
    params = {"key": API_KEY, "start_at": current_date_timestamp}
    response = requests.get(PRODUCTION_URL, params=params, headers=header)
    body = response.json()
    # Cleanse and load data into list of dictionaries
    try:
        for interval in body['intervals']:
            if interval['enwh'] != 0:
                data.append({'time': datetime.strftime(datetime.fromtimestamp(interval['end_at']), "%Y-%m-%d %H:%M:%S"),
                                        'production': interval['enwh']})
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
        result = requests.get("http://" + SERVER_IP + "/solar/production/last-update")
        last_record = result.json()['last_entry']
        last_date = datetime.strptime(last_record, '%a, %d %b %Y %H:%M:%S %Z').date()
        # Set the current date to start data request
        current_date = last_date + timedelta(days=1) # start date will be the day after most recent in database
        current_date_timestamp = int(time.mktime(current_date.timetuple()))  # Convert to timestamp to work with enphase
        # Get production data for current date, append to table, and then move to next day
        while current_date <= datetime.today():
            data = get_production_data_from_select_day(current_date_timestamp)
            
            response = requests.post("http://" + SERVER_IP + f"/solar/production/{current_date}", json={"days_production": data})
            if response.status_code != 200:
                discord = Discord(url=CHANNEL_URL)
                discord.post(content=f"Error in back-end publishing data for {current_date}")

            # data.to_sql('enphase_production', ACCESS_DATA.db, if_exists='append')
            current_date_timestamp = current_date_timestamp + 86400
            current_date = current_date + timedelta(days=1)

    except Exception as error:
        discord = Discord(url=CHANNEL_URL)
        discord.post(content=f"Enphase data was either partially recorded or not recorded at all!\nError: {error}")
