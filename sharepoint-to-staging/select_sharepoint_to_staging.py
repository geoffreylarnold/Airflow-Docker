#!/usr/bin/env python
# coding: utf-8
from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
import requests
import json
import urllib.parse
import pandas as pd
import os
import sqlalchemy as sa
import time
from sqlalchemy.engine import URL

# Load Datawarehouse Credentials
wh_host = os.getenv("wh_host")
wh_db = os.getenv("wh_db")
wh_un = os.getenv("wh_user")
wh_pw = os.getenv("wh_pass")
schema = os.getenv("schema", 'Staging')
columns = os.getenv("columns")
v_force = os.getenv("varchar_force")

sheet = os.getenv("sheet", '')
skip = int(os.getenv('skip', 0))
filetype = os.getenv("file_type", '')

# Build Connection & Query Warehouse
connection_url = URL.create(
    "mssql+pyodbc",
    username=wh_un,
    password=wh_pw,
    host=wh_host,
    database=wh_db,
    query={
        "driver": "ODBC Driver 17 for SQL Server"
    },
)
engine = sa.create_engine(connection_url)

client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")

# Save location
dept = os.getenv('dept')
source = os.getenv('source', 'Sharepoint')
table = os.getenv('table')
table_name = '{}_{}_{}'.format(dept, source, table)

# Information
drive = os.getenv('drive_id')
file_id = os.getenv('file_id')
drive_type = os.getenv('drive_type', 'drives')

auth = HTTPBasicAuth(client_id, client_secret)
client = BackendApplicationClient(client_id=client_id)
oauth = OAuth2Session(client=client)

# Auth
token = oauth.fetch_token(
    token_url='https://login.microsoftonline.com/e0273d12-e4cb-4eb1-9f70-8bba16fb968d/oauth2/v2.0/token',
    scope='https://graph.microsoft.com/.default',
    auth=auth)

bearer = "Bearer {}".format(token['access_token'])
headers = {'authorization': bearer}

# Column Specs

# lines used for defining file location for import, local testing
# f = open(columns)
# columns_select = json.load(f)
# column_list = [sub['COLUMN_NAME'] for sub in columns_select]

columns_select = json.loads(columns)
column_list = [sub['COLUMN_NAME'] for sub in columns_select]
# columns_dtype = [sub['DATA_TYPE'] for sub in columns_select]
# columns_dtype = list(map(str.upper, columns_dtype))
# column_converter = dict(zip(column_list, columns_dtype))

if not v_force:
    v_force = None
else:
    v_force = v_force.split()

if v_force is not None:
    v_force_list = {}
    for i in v_force:
        v_force_list[i] = sa.types.VARCHAR
else:
    v_force_list = None

if drive_type == 'drives':
    url = "https://graph.microsoft.com/v1.0/drives/{}/items/{}/content".format(drive, file_id)
else:
    url = "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}/content".format(drive, file_id)

file = requests.request("GET", url, data="", headers=headers)

if filetype == 'x':
    filename = 'test.xlsx'
else:
    filename = 'test.xls'

with open(filename, 'wb') as output:
    output.write(file.content)

if sheet == '':
    df = pd.read_excel(filename, usecols=column_list)
elif sheet == 'all':
    xls = pd.ExcelFile(filename)
    df = pd.DataFrame()
    for i in xls.sheet_names:
        temp = pd.read_excel(filename, sheet_name=i, usecols=column_list)
        df = df.append(temp)
else:
    if skip == 0:
        df = pd.read_excel(filename, sheet_name=sheet, usecols=column_list)
    else:
        df = pd.read_excel(filename, sheet_name=sheet, skiprows=range(0, skip),
                           usecols=column_list)

if filetype != 'x':
    df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

print(df.head())
# test_types = dict(zip(
#     df.columns.tolist(),
#     (types.VARCHAR(length=20), types.Integer(), types.Float()) ))

df.to_sql(name=table_name, schema=schema, con=engine, if_exists='replace', index=False, dtype=v_force_list)
