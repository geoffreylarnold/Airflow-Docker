#!/usr/bin/env python
# coding: utf-8
import os
import pandas as pd
import sqlalchemy as sa
import pantab as pt
import tableauserverclient as TSC
import sys
from tableauhyperapi import TableName
from tableauhyperapi import Connection
import re
import gc
from sqlalchemy.engine import URL
import zipfile
import warnings

dev = "YES"

if dev == "YES":
    from dotenv import load_dotenv

    load_dotenv("./tableau-transfer/.env")
    # os.chdir("./tableau-transfer")

# Env Variables
# Data Vars
dept = os.getenv('dept')
table = os.getenv('table')
schema = os.getenv('schema', 'Reporting')
column_q = os.getenv('column_q', '*')
fix_dates = os.getenv('fix_dates', 'yes')
int_requests = os.getenv('INT_REQ', '')
int_chunks = int(os.getenv('INT_CHUNKSIZE', 5000))
append_column = os.getenv('APPEND_COLUMN', None)

# Tableau Vars
if dev != "YES":
    name = os.getenv('name')
else:
    name = "{}_Test".format(os.getenv('name'))
    # name = os.getenv('name')

mode = os.getenv('mode', 'Append')
project_id = os.getenv('project_id', '')
if dev != "YES":
    project_name = os.getenv('project_name', dept)
else:
    project_id = '0a11c9fa-a584-42f4-a3f8-58c0e1b39e03'
    project_name = "Site Archive"

site = os.getenv('site', 'CountyStats')
server = os.getenv('server', 'tableau')

# Load Tableau Credentials
tableau_username = os.getenv("ts_username")
tableau_password = os.getenv("ts_password")
tableau_TOKEN_name = os.getenv("TS_TOKEN_NAME")
tableau_TOKEN = os.getenv("TS_TOKEN")

# Load Datawarehouse Credentials
wh_host = os.getenv("wh_host")
wh_db = os.getenv("wh_db")
wh_un = os.getenv("wh_user")
wh_pw = os.getenv("wh_pass")

# Build Connection & Query Warehouse
if dev == "NO":
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

# Connect to Tableau Server
server = TSC.Server('https://{}.alleghenycounty.us'.format(server))
server.version = '3.3'
if dev == "NO":
    tableau_auth = TSC.TableauAuth(tableau_username, tableau_password, site_id=site)
else:
    tableau_auth = TSC.PersonalAccessTokenAuth(tableau_TOKEN_name,
                                               tableau_TOKEN, site_id=site)

# Find if data source exist
with server.auth.sign_in(tableau_auth):
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, name))
    all_datasources, pagination_item = server.datasources.get(req_option)
    if len(all_datasources) == 0:
        datasource_id = ''
    elif str(all_datasources[0]) != '[]' and len(all_datasources) == 1:
        first = all_datasources[0]
        datasource_id = first.id
    elif str(all_datasources[0]) != '[]' and len(all_datasources) > 1:
        for x in all_datasources:
            if x.project_name == project_name:
                datasource_id = x.id
            elif x.project_id == project_id:
                datasource_id = x.id
            else:
                sys.exit('More than one datasource exists with name {} within folder {}. Please verify datasource '
                         'name and project folder/id.'.format(name, project_name))

# Upload format dependent on presence of matching datasource id/name or project id/name
if datasource_id != '':
    with server.auth.sign_in(tableau_auth):
        datasource_download = server.datasources.download(datasource_id)
    with zipfile.ZipFile('{}.tdsx'.format(name), 'r') as zip_ref:
        zip_ref.extractall()
    query = """
    SELECT DISTINCT concat("Date_Worked", "JDE_ID", "Pay_Type")
    FROM "Extract"."Extract"
    """
    df = pt.frame_from_hyper_query('./Data/Extracts/{}.hyper'.format(name), query) #STOPPED HERERERER
    
    hyper_extract = pt.frames_from_hyper('./Data/Extracts/{}.hyper'.format(name))
    current_data = pd.DataFrame(list(hyper_extract.values())[0])
elif (project_id != '' and datasource_id == '') or (project_id != '' and datasource_id == ''):
    mode = "CreateNew"
    warnings.warn('No current datasource with name {} exists. Datasource will be created within specified project ID: '
                  '{}'.format(name, project_id))
    if project_id == '':
        with server.auth.sign_in(tableau_auth):
            req_option = TSC.RequestOptions()
            req_option.filter.add(
                TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, project_name))
            all_project_items, pagination_item = server.projects.get(req_option)
            parse = list([proj.id for proj in all_project_items])
            project_id = str(parse[0])
            print('Found Project ID ({}).'.format(project_id), file=sys.stderr)
    upload = TSC.DatasourceItem(project_id, name=name)
    current_data = None
else:
    mode = 'CreateNew'
    with server.auth.sign_in(tableau_auth):
        req_option = TSC.RequestOptions()
        req_option.filter.add(
            TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, project_name))
        all_project_items, pagination_item = server.projects.get(req_option)
        parse = list([proj.id for proj in all_project_items])
        project_id = str(parse[0])

    print('Found Project ID ({}).'.format(project_id), file=sys.stderr)
    warnings.warn('No current datasource with name {} exists. Datasource will be created within specified project ID: '
                  '{}'.format(name, project_id))
    upload = TSC.DatasourceItem(project_id, name=name)

# Pull Date and DateTime Columns
date_cols = """SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{}' AND DATA_TYPE IN ('date', 'datetime', 'smalldatetime')""".format(table)

cols = pd.read_sql_query(date_cols, engine)

datecols = cols["COLUMN_NAME"].values.tolist()

int_cols = """SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{}' AND DATA_TYPE IN ('int')""".format(table)

cols = pd.read_sql_query(int_cols, engine)

intcols = cols["COLUMN_NAME"].values.tolist()

# Create New Data Upload if Current Data Doesn't Exist
if current_data is None:
    # Read and write table to hyper file
    print('Extracting Data to Hyper file.', file=sys.stderr)
    count = 0
    for df in pd.read_sql_query("SELECT {} FROM {}.{}".format(column_q, schema, table), engine, chunksize=int_chunks):
        print(f'Running chunk {count}', file=sys.stderr)
        # Avoid issues with numerous nulls in datetime columns
        if fix_dates == 'yes':
            print('Fixing Dates', file=sys.stderr)
            for col in list(df):
                if col in datecols:
                    print(f'Set {col} to dateimte', file=sys.stderr)
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    # Remove Timezone for Hyper file
        for col in df.select_dtypes('datetimetz').columns:
            print(f'Fixing {col} timezone', file=sys.stderr)
            df[col] = df[col].dt.tz_convert(None)
        # Make all integers float for consistency (pandas guesses wrong with chunking)
        for col in intcols:
            print(f'Setting {col} to float', file=sys.stderr)
            df[col] = df[col].astype(float)
        if int_requests != '':
            int_req = int_requests.split(",")
            int_req = [x for x in int_req if x not in intcols]
            if len(int_req) > 0:
                for col in int_req:
                    print(f'Setting {col} to float', file=sys.stderr)
                    df[col] = df[col].astype(float)
        if count > 0:
            pt.frame_to_hyper(df, "temp.hyper", table=TableName("Extract", "Extract"), table_mode="a")
            print('Completed Chunk {}.'.format(count), file=sys.stderr)
            count += 1
        else:
            pt.frame_to_hyper(df, "temp.hyper", table=TableName("Extract", "Extract"))
            print('Completed Chunk {}.'.format(count), file=sys.stderr)
            count = 1
        gc.collect()
    # publish data source (specified in file_path)
    with server.auth.sign_in(tableau_auth):
        server.datasources.publish(upload, r'temp.hyper', mode)
    print("New upload performed for {} within {}".format(name, project_id))
    sys.exit(0)

# Create lookup table of current Append_Column
# append_column_current =

# Read and write table to hyper file



# publish data source (specified in file_path)
with server.auth.sign_in(tableau_auth):
    server.datasources.publish(upload, r'temp.hyper', mode)
