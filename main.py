#!/usr/bin/enphase-tracker/venv/bin/python3

import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, date
import time
import requests
from discordwebhook import Discord
import psycopg2
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

from accessInstance import AccessInstance

# Loading the .env file from the project
load_dotenv()

# Setting all globals
API_KEY = os.environ.get("ENPHASE_KEY")
SYSTEM_ID = os.environ.get("SYSTEM_ID")
CHANNEL_URL = os.getenv('ENPHASE_CHANNEL_URL')
POOL_CHANNEL_URL = os.getenv('POOL_CHANNEL_URL')
SERVER_IP = os.environ.get("SERVER_IP")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")

SYSTEM_URL = f"api.enphaseenergy.com/api/v4/systems/{SYSTEM_ID}/summary?key={API_KEY}"
PRODUCTION_URL = f'https://api.enphaseenergy.com/api/v4/systems/{SYSTEM_ID}/telemetry/production_micro'

def get_daily_pool_temp_graph():
    # days_ago = 0
    today = datetime.today().date()
    # start_date = today - timedelta(days=days_ago)
    start_date = today
    end_date = start_date + timedelta(days=1)
    query = f"""SELECT pooldata.datetime,
        pooldata.roof_temp,
        pooldata.water_temp,
        pooldata.valve,
        pooldata.temp_range
    FROM pooldata
    WHERE pooldata.datetime > '{start_date} 17:00:00+00'::timestamp with time zone AND pooldata.datetime < '{end_date} 02:00:00+00'::timestamp with time zone
    ORDER BY pooldata.datetime DESC;"""

    # query = "SELECT * from pool23apr"
    # Establish connection to PostgreSQL database
    with psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS) as conn:
            
            cur = conn.cursor()
            cur.execute(query)


            data = cur.fetchall()

            # Close database connection
            cur.close()

    # Separate data into two lists for each column
    column1_data = [row[1] for row in data]
    column2_data = [row[2] for row in data]
    times = [row[0] for row in data]

    # Plot the data in one graph using Matplotlib
    plt.plot(times, column1_data, label='Roof')
    plt.plot(times, column2_data, label='Pool')
    plt.xlabel('Time (UTC)')
    plt.ylabel('Temp (F)')
    plt.legend()
    filename = "pool_today.jpg"
    plt.savefig(filename)
    discord = Discord(url=POOL_CHANNEL_URL)
    discord.post(file={"Frame": open(filename, "rb"),})

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
        get_daily_pool_temp_graph()
    except Exception:
        discord = Discord(url=POOL_CHANNEL_URL)
        discord.post(content=f"Pool data was either inaccessible or could not be compiled!\nError: {error}")
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
        while current_date <= date.today():
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
