import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import base64
import requests
from datetime import datetime
import pytz

load_dotenv()

CLIENT_ID = os.environ.get("ENPHASE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("ENPHASE_CLIENT_SECRET")
client_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
client_bytes = client_string.encode("ascii")
base64_bytes = base64.b64encode(client_bytes)
AUTH_KEY = base64_bytes.decode("ascii")
DB_STRING = os.environ.get('DB_STRING')
ACCESS_URL = f"https://api.enphaseenergy.com/oauth/token?grant_type=authorization_code&redirect_uri=https://api.enphaseenergy.com/oauth/redirect_uri&code="

db = create_engine(DB_STRING)

user_code = input("Enter new session code: ")
header = {'Authorization': "Basic " + AUTH_KEY}
response = requests.post(ACCESS_URL + user_code, headers=header)
body = response.json()
access_token = body['access_token']
refresh_token = body['refresh_token']
data = [{'at': access_token, 'rt': refresh_token, 'acdate': datetime.now(pytz.UTC), 'rfdate': datetime.now(pytz.UTC), 'user': user_code}]
df = pd.DataFrame(data)
df.set_index(['user'], inplace=True)
df.to_sql('sd_access', db, if_exists='replace')
