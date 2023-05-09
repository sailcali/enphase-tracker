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
CHANNEL_URL = os.getenv('ENPHASE_CHANNEL_URL')
ENPHASE_CODE= os.environ.get("ENPHASE_CODE")
SERVER_IP= os.environ.get("SERVER_IP")
SERVER_API_PWD= os.environ.get("API_PASSWORD")

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

    user_code = ENPHASE_CODE
    access_token = None
    refresh_token = None
    refresh_date = None
    access_date = None
    
    def __init__(self):
        """On startup, we will create our reusable database connection
        then fetch current token data from database"""

        self.get_access_data()
    
    def get_access_data(self):
        """Fetch current token data from database, then verify age"""
        
        response = requests.get("http://" + SERVER_IP + "/solar/access", 
                            headers={"password": SERVER_API_PWD},
                            json={"user": self.user_code})
        data = response.json()['keys']
        date = datetime.strptime(data['date'], '%a, %d %b %Y %H:%M:%S %Z')
        date = date.replace(tzinfo=pytz.UTC)
        self.access_token = data["at"]
        self.refresh_token = data["rt"]
        self.access_date = date
        self.refresh_date = date
        self.verify_instance()

    def verify_instance(self):
        """If access token is older than a day, get a new one
        If refresh token is older than a week, get a new code from user"""
        try:
            access_age = datetime.now(pytz.UTC) - self.access_date
            refresh_age = datetime.now(pytz.UTC) - self.refresh_date
        except TypeError as error:
            print(error)
            quit()
        if access_age.total_seconds() >= SECONDS_PER_DAY:
            if refresh_age.total_seconds() >= SECONDS_PER_WEEK:
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
        data = {'at': access_token, 'rt': refresh_token, 'date': datetime.strftime(datetime.now(pytz.UTC), '%a, %d %b %Y %H:%M:%S %Z'), 'user': self.user_code}
        response = requests.post("http://" + SERVER_IP + "/solar/access",
                                 headers={'password': SERVER_API_PWD},
                                 json=data)
        self.get_access_data()

    def __str__(self):
        return f"Code: {self.user_code}\nToken: {self.access_token[-5:]}\nRefresh: {self.refresh_token[-5:]}\nAccess Date: {self.access_date}\nRefresh Date: {self.refresh_date}"