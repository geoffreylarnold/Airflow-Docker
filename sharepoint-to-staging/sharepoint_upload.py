from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
import requests
import json
import urllib.parse
import pandas as pd
import os
import re
import calendar
from datetime import date, timedelta, datetime

# from dotenv import load_dotenv
#
# load_dotenv()

client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")
sp_folder_name = os.getenv("target_folder_name", None)
filename = os.getenv("filename")
dest_filename = os.getenv("dest_filename", filename)
mount_path = os.getenv("source_folder_name", "/CountyExec")
drive = os.getenv("drive_id")

# Create Auth object
auth = HTTPBasicAuth(client_id, client_secret)
client = BackendApplicationClient(client_id=client_id)
oauth = OAuth2Session(client=client)

token = oauth.fetch_token(
    token_url='https://login.microsoftonline.com/e0273d12-e4cb-4eb1-9f70-8bba16fb968d/oauth2/v2.0/token',
    scope='https://graph.microsoft.com/.default',
    auth=auth,
    verify=False)

bearer = "Bearer {}".format(token['access_token'])
# Build headers
payload = ""
headers = {'authorization': bearer}
# Find the root folder to upload to
root_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/root".format(drive)
root_r = requests.request("GET", root_url, data=payload, headers=headers, verify=False)
root = root_r.json()['id']

items_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}/children".format(drive, root)
items_r = requests.request("GET", items_url, data=payload, headers=headers)
children = pd.DataFrame.from_dict(items_r.json()['value'])
# Upload the file to the correct folder location
if sp_folder_name is None or children.empty:
    if children.empty:
        print("No subfolders present: defaulting to Doucments root folder")
    put_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/root:/{}:/content".format(drive, dest_filename)
else:
    folder_id = children[children['name'] == sp_folder_name].iloc[0]['id']
    put_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}:/{}:/content".format(drive, folder_id, dest_filename)
upload_headers = headers = {'authorization': bearer, 'Content-Type': 'application/json'}
upload = requests.request("PUT", put_url, data=open(f"{mount_path}{filename}", 'rb'), headers=headers)
try:
    upload.raise_for_status()
except requests.exceptions.HTTPError as err:
    print(err)
