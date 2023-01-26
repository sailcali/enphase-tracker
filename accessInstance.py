from sqlalchemy import create_engine
import psycopg2
import pandas as pd
from datetime import datetime
import pytz
from dotenv import load_dotenv
import os
import base64
import requests
from discordwebhook import Discord

SECONDS_PER_DAY = 86400 - 60
SECONDS_PER_WEEK = 604800 - 60

# Loading the .env file from the project
load_dotenv()

# Setting all globals
API_KEY = os.environ.get("ENPHASE_KEY")
SYSTEM_ID = os.environ.get("SYSTEM_ID")
DB_STRING = os.environ.get('DB_STRING')
DB_PASS = os.environ.get("DB_PASS")
CHANNEL_URL = os.getenv('ENPHASE_CHANNEL_URL')

CLIENT_ID = os.environ.get("ENPHASE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("ENPHASE_CLIENT_SECRET")
client_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
client_bytes = client_string.encode("ascii")
base64_bytes = base64.b64encode(client_bytes)
AUTH_KEY = base64_bytes.decode("ascii")

ACCESS_URL = f"https://api.enphaseenergy.com/oauth/token?grant_type=authorization_code&redirect_uri=https://api.enphaseenergy.com/oauth/redirect_uri&code="
REFRESH_URL = f"https://api.enphaseenergy.com/oauth/token?grant_type=refresh_token&refresh_token="

class AccessInstance:
    """Main class for managing the API tokens stored in the database. 
    Will keep refreshed for up to a week. 
    After a week, user will need to sign back in and get a new access code"""

    user_code = None
    access_token = None
    refresh_token = None
    refresh_date = None
    access_date = None
    
    def __init__(self):
        """On startup, we will create our reusable database connection
        then fetch current token data from database"""

        self.db = create_engine(DB_STRING)
        self.get_access_data()
    
    def get_access_data(self):
        """Fetch current token data from database, then verify age"""
        
        with self.db.connect() as con:
            sql = 'SELECT * FROM sd_access;'
            result = con.execute(sql)
            data = result.fetchone()
        print(data)
        self.user_code = data[0]
        self.access_token = data[1]
        self.refresh_token = data[2]
        self.access_date = data[3]
        self.refresh_date = data[4]
        self.verify_instance()

    def verify_instance(self):
        """If access token is older than a day, get a new one
        If refresh token is older than a week, get a new code from user"""

        access_age = datetime.now(pytz.UTC) - self.access_date
        refresh_age = datetime.now(pytz.UTC) - self.refresh_date
        if access_age.total_seconds() > SECONDS_PER_DAY:
            if refresh_age.total_seconds() > SECONDS_PER_WEEK:
                discord = Discord(url=CHANNEL_URL)
                discord.post(content=f"Enphase refresh token expired!")
            else:
                print("refresh")
                self.get_refresh_token()

    def get_refresh_token(self):
        """Fetch new access token from API using refresh token. Returns a new refresh token and posts to database
        Will automatically retrieve the data and verify"""

        header = {'Authorization': "Basic " + AUTH_KEY}
        response = requests.post(REFRESH_URL + self.refresh_token, headers=header)
        body = response.json()
        access_token = body['access_token']
        refresh_token = body['refresh_token']
        data = {'at': access_token, 'rt': refresh_token, 'acdate': datetime.now(pytz.UTC), 'rfdate': datetime.now(pytz.UTC), 'user': self.user_code}
        # df = pd.DataFrame(data)
        # df.set_index(['user'], inplace=True)
        # df.to_sql('sd_access', self.db, if_exists='replace')
        conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format('127.0.0.1', '5432', 'kiowa-monitor', 'postgres', DB_PASS))
        sql = f"""UPDATE sd_access SET at = '%s', rt = '%s', acdate = %s, rfdate = %s WHERE user = '%s';"""
        csr = conn.cursor() 
        csr.execute(sql, (data['at'],data['rt'],data['acdate'],data['rfdate'],self.user_code))
        conn.commit()
        csr.close()
        conn.close()
        self.get_access_data()

    def __str__(self):
        return f"Code: {self.user_code}\nToken: {self.access_token[-5:]}\nRefresh: {self.refresh_token[-5:]}\nAccess Date: {self.access_date}\nRefresh Date: {self.refresh_date}"