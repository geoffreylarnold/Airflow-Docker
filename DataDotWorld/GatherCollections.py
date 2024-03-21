import json
import os
import pandas as pd
import sqlalchemy as sa
import pantab
import sys
import urllib3.exceptions
from urllib3.exceptions import NewConnectionError
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session
import requests
from json import loads
from send_email import send_email
import time
from requests.exceptions import SSLError
from requests.adapters import HTTPAdapter, Retry
from sqlalchemy import delete, Table, MetaData, insert, select

# Change to "NO" when in Dev/Prod servers
dev = "YES"

if dev == "YES":
    from dotenv import load_dotenv

    load_dotenv(".env")

# Load Datawarehouse Credentials
wh_host = os.getenv("WH_HOST")
wh_db = os.getenv("WH_DB")
wh_un = os.getenv("WH_USER")
wh_pw = os.getenv("WH_PASS")

# Import table variables
dept = os.getenv('DEPT')
table = os.getenv('TABLE', 'TableCatalog')
source = os.getenv('SOURCE')
schema = os.getenv('SCHEMA', 'Staging')

# Message variables
email_filename = os.getenv('EMAIL_TEMPLATE')
email_subject = os.getenv('EMAIL_SUBJECT')
image_subfolder = os.getenv('IMAGE_SUBFOLDER')

# DDW variables
auth_token = os.getenv('DW_AUTH_TOKEN')

datadotworld = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
datadotworld.mount('http://', HTTPAdapter(max_retries=retries))

# API call
headers = {
    'Accept': "application/json",
    'Authorization': "Bearer {}".format(auth_token)
}

# Gather data sources to use for tables API call
response = []
while True:
    try:
        response = \
            datadotworld.get("https://api.data.world/v0/metadata/data/sources/alleghenycounty?size=1000",
                             headers=headers)
        break
    except (SSLError, ConnectionError, TimeoutError):
        continue
d = loads(response.text)
df_datasources = pd.DataFrame(d['records'])

source_records = pd.DataFrame()
for i, row in df_datasources.iterrows():
    sourceid = row['id']
    attempt = 0
    while True:
        try:
            attempt += 1
            response = datadotworld.get(
                "https://api.data.world/v0/metadata/data/sources/alleghenycounty/{}/tables?size=20".
                format(sourceid),
                headers=headers)
            print(response.url, "[", attempt, "]")
            d = loads(response.text)
            df_d = pd.json_normalize(d, 'records')
            df_d['Source'] = sourceid
            break
        except (SSLError, ConnectionError, KeyError, NewConnectionError):
            continue
        except TimeoutError:
            time.sleep(1)
            continue

    source_records = pd.concat([df_d, source_records], ignore_index=True)
    while "nextPageToken" in response.json():
        next_page = d['nextPageToken']
        while True:
            try:
                response = datadotworld.get("""https://api.data.world/v0/{}""".format(next_page), headers=headers)
                print("Next page add: ", next_page)
                d = loads(response.text)
                df_d = pd.json_normalize(d, 'records')
                df_d['Source'] = sourceid
                break
            except (SSLError, ConnectionError, KeyError, NewConnectionError):
                continue
            except TimeoutError:
                time.sleep(1)
                continue
        source_records = pd.concat([df_d, source_records], ignore_index=True)
    time.sleep(0.1)

source_records_n = source_records[['title', 'collections', 'Source']]
source_records_n = source_records_n.explode('collections')
source_records_n['Collection'] = [d.get('collectionId') for d in source_records_n.collections]

# Gather IRI
# Gather catalog records of database tables using collection name, which then gives table IRI for eventual url link
catalog_records = pd.DataFrame()

for collection in df_datasources['id']:
    attempt = 0
    while True:
        try:
            attempt += 1
            response = \
                datadotworld.get("https://api.data.world/v0/metadata/data/sources/alleghenycounty/{}/tables?size=1000".
                                 format(collection), headers=headers)
            print(response.url, "[", attempt, "]")
            d = loads(response.text)
            df_d = pd.DataFrame(d['records'])
            break
        except (SSLError, ConnectionError, TimeoutError, KeyError, NewConnectionError):
            continue
    catalog_records = pd.concat([df_d, catalog_records], ignore_index=True)

catalog_IRI = catalog_records.explode('collections')
catalog_IRI = catalog_IRI[['id', 'encodedIri', 'collections']]
catalog_IRI = pd.concat([catalog_IRI.drop(['collections'], axis=1),
                         catalog_IRI['collections'].apply(pd.Series)], axis=1)

# Merge unique table names and data steward info with catalog records (brings in data table IRI)
df_tables_n = source_records_n.drop(['collections'], axis=1).merge(catalog_IRI, left_on=['title', 'Collection'], right_on=['id', 'collectionId'],
                                     how='left').drop_duplicates()

# Build Connection & Query Warehouse
if dev == "NO":
    print("Using CountyStat Username")
    connection_url = URL.create(
        "mssql+pyodbc",
        username=wh_un,
        password=wh_pw,
        host=wh_host,
        database=wh_db,
        query={
            "driver": "ODBC Driver 17 for SQL Server",
        },
    )

    engine = sa.create_engine(connection_url)
else:
    engine = sa.create_engine(
        "mssql+pyodbc://{}/{}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server".format(wh_host, wh_db))

table_name = "{}_{}_{}".format(dept, source, table)
df_tables_n.to_sql(name=table_name, con=engine, schema=schema, if_exists="replace")
