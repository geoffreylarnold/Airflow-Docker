#!/usr/bin/env python
# coding: utf-8
import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL
import geopandas as gpd
import requests
from dotenv import load_dotenv

load_dotenv('./gis-to-staging/.env')

un = os.getenv('LOGIN')
pwd = os.getenv('PASSWORD')

url = "https://www.arcgis.com/sharing/generateToken"

payload = f"-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"username\"\r\n\r\n{un}\r\n-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"password\"\r\n\r\n{pwd}\r\n-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"referer\"\r\n\r\nhttps://www.arcgis.com\r\n-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"f\"\r\n\r\njson\r\n-----011000010111000001101001--\r\n"
headers = {
    "Content-Type": "multipart/form-data; boundary=---011000010111000001101001",
    "f": "json"
}

response = requests.request("POST", url, data=payload, headers=headers)

token = response.json()['token']

service = os.getenv("SERVICE")
offset = 0
gis_url = f"{service}query?where=1%3D1&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&f=pgeojson&token={token}"

gdf = gpd.read_file(gis_url)
while gdf.shape[0] % 2000 == 0:
    offset+=2000
    print(offset)
    gis_url_w = f"{service}query?where=1%3D1&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&resultOffset={offset}&f=pgeojson&token={token}"
    print(gis_url_w)
    gdf_t = gpd.read_file(gis_url_w)
    gdf = pd.concat([gdf, gdf_t], ignore_index=True)

# Build Connection & Query Warehouse
connection_url = URL.create(
                    "mssql+pyodbc",
                    username=os.getenv("wh_user"),
                    password=os.getenv("wh_pass"),
                    host=os.getenv("wh_host"),
                    database=os.getenv("wh_db"),
                    query={
                        "driver": "ODBC Driver 17 for SQL Server"
                    },
                )
engine = sa.create_engine(connection_url)

