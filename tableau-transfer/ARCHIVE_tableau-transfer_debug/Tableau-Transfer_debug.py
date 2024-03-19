#!/usr/bin/env python
# coding: utf-8
import os
import pandas as pd
import sqlalchemy as sa
import pantab 
import tableauserverclient as TSC
import sys
from tableauhyperapi import TableName
import re
import gc

# Env Variables
# Data Vars
dept = os.getenv('dept')
table = os.getenv('table')
schema = os.getenv('schema', 'Reporting')
fix_dates = os.getenv('fix_dates', 'yes')

# Tableau Vars
name = os.getenv('name')
mode = os.getenv('mode', 'Overwrite')
project_id = os.getenv('project_id', '')
project_name = os.getenv('project_name', dept)
site = os.getenv('site', 'CountyStats')
server = os.getenv('server', 'tableau')

# Load Tableau Credentials
tableau_username = os.getenv("ts_username")
tableau_password = os.getenv("ts_password")

# Load Datawarehouse Credentials
wh_host = os.getenv("wh_host")
wh_db = os.getenv("wh_db")
wh_un = os.getenv("wh_user")
wh_pw = os.getenv("wh_pass")

# Build Connection & Query Warehouse
wh_conn_string = "mssql+pyodbc://{}:{}@{}/{}?driver=ODBC+Driver+17+for+SQL+Server".format(wh_un, wh_pw, wh_host, wh_db)
engine = sa.create_engine(wh_conn_string)

# Read and write table to hyper file
print('Extracting Data to Hyper file.', file=sys.stderr)
count = 0
for df in pd.read_sql_table(table, engine, schema = schema, chunksize = 100000):
    # Avoid issues with numerous nulls in datetime columns
    if fix_dates == 'yes':
        for col in list(df):
            if bool(re.search("date", col.lower()) or re.search("time", col.lower())):
                df[col] = df[col]= pd.to_datetime(df[col]) 
    # Remove Timezone for Hyper file
    for col in df.select_dtypes('datetimetz').columns:
        df[col] = df[col].dt.tz_convert(None)
    # Make all integers float for consistency (pandas guesses wrong with chunking)
    for col in df.select_dtypes('int64').columns:
        df[col] = df[col].astype(float)
    if count > 0:
        pantab.frame_to_hyper(df, "../temp.hyper", table=TableName("Extract", "Extract"), table_mode ="a")
        count += 1
        print('Completed Chunk {}.'.format(count), file=sys.stderr)
    else: 
        pantab.frame_to_hyper(df, "../temp.hyper", table=TableName("Extract", "Extract"))
        count = 1    
    gc.collect()

# Connect to Tableau Server
server = TSC.Server('https://{}.alleghenycounty.us'.format(server))
server.version = '3.3'
tableau_auth = TSC.TableauAuth(tableau_username, tableau_password, site_id= site)

# Find if data source exist
with server.auth.sign_in(tableau_auth):
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, name))
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.ProjectName, TSC.RequestOptions.Operator.Equals, project_name))
    all_datasources = server.datasources.get(req_option)
    if str(all_datasources[0]) != '[]':
        first = all_datasources[0][0]
        first_check = str(type(first)) == """<class 'tableauserverclient.models.datasource_item.DatasourceItem'>""" and first._datasource_type == 'hyper'
    else:
        first_check = False

# Find project ID if resource doesn't exist and project ID was not provided
if project_id == '' and not first_check:
    with server.auth.sign_in(tableau_auth):
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, project_name))
        all_project_items, pagination_item = server.projects.get(req_option)
        parse = list([proj.id for proj in all_project_items])
        project_id = str(parse[0])

print('Found Project ID ({}).'.format(project_id), file=sys.stderr)

# Final checks and settings before Upload
if len(all_datasources) == 2 and first_check:
    upload = first
elif project_id != '':
    upload = TSC.DatasourceItem(project_id, name = name)
elif project_id == '' and project_name =='':
    sys.exit('No projects exist with project name {}. Create this project or choose a different project name'.format(project_name))
elif len(parse) > 1:
    sys.exit('More than one projects exist with project name {}. Please pass the projects API id.'.format(project_name))
else:
    mode = 'CreateNew'
    upload = TSC.DatasourceItem(project_id, name = name)

print('Writing Hyperfile to Tableau Server.', file=sys.stderr)
    
# publish data source (specified in file_path)
with server.auth.sign_in(tableau_auth):
    server.datasources.publish(upload, r'temp.hyper', mode)
