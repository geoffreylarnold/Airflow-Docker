import json
import os
import pandas as pd
import sqlalchemy as sa
import pantab
import sys
from sqlalchemy.engine import URL
import requests
from json import loads
from send_email import send_email
import time

# Change to "NO" when in Dev/Prod servers
dev = "NO"

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
table = os.getenv('TABLE')
source = os.getenv('SOURCE')
schema = os.getenv('SCHEMA', 'Master')

# Message variables
email_filename = os.getenv('EMAIL_TEMPLATE')
email_subject = os.getenv('EMAIL_SUBJECT')
image_subfolder = os.getenv('IMAGE_SUBFOLDER')

# DDW variables
auth_token = os.getenv('DW_AUTH_TOKEN')

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

# Set table name for data import from Warehouse
table_name = "{}_{}_{}".format(dept, source, table)
df = pd.read_sql_table(table_name, engine, schema=schema)

# Gather unique table names
df_tables = df.groupby("Datatable_Title_value").sample(1).drop(['ColumnTitle_value', 'CatalogObject_value',
                                                                'resourceType_value'], axis=1)
# Gather unique collection names
df_collections = df_tables["CollectionName_value"].unique().tolist()

# API call
headers = {
    'Accept': "application/json",
    'Authorization': "Bearer {}".format(auth_token)
}

# Gather catalog records of database tables using collection name, which then gives table IRI for eventual url link
catalog_records = pd.DataFrame()

for collection in df_collections:
    response = requests.get(
        "https://api.data.world/v0/metadata/data/sources/alleghenycounty/{}/tables?size=1000".format(collection),
        headers=headers)
    d = loads(response.text)
    df_d = pd.DataFrame(d['records'])
    catalog_records = pd.concat([df_d, catalog_records], ignore_index=True)

catalog_IRI = catalog_records.explode('collections')
catalog_IRI = catalog_IRI[['id', 'encodedIri', 'collections']]
catalog_IRI = pd.concat([catalog_IRI.drop(['collections'], axis=1),
                         catalog_IRI['collections'].apply(pd.Series)], axis=1)

# Gather catalog records of column metadata using unique table names, which then gives column IRI for eventual url link
column_data = pd.DataFrame()

for tablet in df_tables['Datatable_Title_value']:
    sourceid = df_tables.loc[df_tables['Datatable_Title_value'] == tablet, ['CollectionName_value']]
    response = requests.get(
        "https://api.data.world/v0/metadata/data/sources/alleghenycounty/{}/tables/{}/columns".
        format(sourceid.iloc[0]['CollectionName_value'],
               tablet),
        headers=headers)
    d = loads(response.text)
    df_d = pd.json_normalize(d, 'records')
    column_data = pd.concat([df_d, column_data], ignore_index=True)
    time.sleep(0.01)

column_IRI = column_data.explode('collections')
column_IRI = column_IRI[['id', 'encodedIri', 'collections']]
column_IRI = pd.concat([column_IRI.drop(['collections'], axis=1),
                        column_IRI['collections'].apply(pd.Series)], axis=1)
column_data = column_data.join(column_IRI["collectionId"], lsuffix="", rsuffix="_x")

# Produce counts of columns per table to use for if else statement in line 137
table_column_count = column_data.groupby(["table.tableId", "collectionId"], as_index=False).size()
table_column_count = table_column_count[table_column_count['size'] < 5]

# Left join Table/Column counts with column title, collection, and column IRI
column_list = table_column_count.merge(column_data[['title', 'table.tableId', 'collectionId', 'encodedIri']],
                                       how='left', left_on=['table.tableId', 'collectionId'],
                                       right_on=['table.tableId', 'collectionId'])

# Merge unique table names and data steward info with cata log records (brings in data table IRI)
df_tables_n = df_tables.merge(catalog_IRI, left_on=['Datatable_Title_value', 'CollectionName_value'],
                              right_on=['id', 'collectionId'])

# Merge (many to one) unique table names, collection name, and data steward to linked column data
column_list = column_list.merge(df_tables_n[['Datatable_Title_value', 'CollectionName_value', 'DataSteward_value']],
                                left_on=['table.tableId', 'collectionId'],
                                right_on=['Datatable_Title_value', 'CollectionName_value'],
                                validate="many_to_one")

# Produce unique list of data stewards with in data warehouse import, and format address to lower case
stewards_table = df_tables_n[['DataSteward_value', 'DataSteward_EMAIL_value']].copy()
stewards_table['DataSteward_EMAIL_value'] = stewards_table['DataSteward_EMAIL_value'].apply(str.lower)
stewards_table = stewards_table.drop_duplicates()
# USED FOR TESTING, COMMENT/DELETE
stewards_table = stewards_table[stewards_table['DataSteward_value'].isin(['Daniel Andrus', 'Justin Wier'
                                                                          'Ali Greenholt', 'Geoffrey Arnold'])]
if dev == "YES":
    stewards_table = stewards_table[stewards_table['DataSteward_value'].isin(['Daniel Andrus'])]

# Opening the html file
HTMLFile = open("""{}/{}""".format(image_subfolder, email_filename), "r")
EmailTemplate = HTMLFile.read()


# Function for creating email message, required tables not in parameter:
# - table_column_count
# - column_list


def message_creater(stewardess, tables, template):
    link_rows = tables[tables['DataSteward_value'] == stewardess]
    if len(link_rows.index) < 7:
        subcol_list = link_rows.merge(table_column_count[['table.tableId', 'collectionId', 'size']], how='left',
                                      left_on=['Datatable_Title_value', 'CollectionName_value'],
                                      right_on=['table.tableId', 'collectionId'])
        subcol_list_filter = subcol_list[(subcol_list['size'].notnull()) & (subcol_list['size'] > 0)]
        if not subcol_list_filter.empty:
            row_html = []
            if len(subcol_list_filter.index) < 5:
                for row in subcol_list_filter.index:
                    sub_bullets_list = subcol_list_filter.merge(column_list, left_on=["CollectionName_value",
                                                                                      "Datatable_Title_value"],
                                                                right_on=["collectionId", "table.tableId"])
                    sub_bullet_html = []
                    for sub in sub_bullets_list.index:
                        sub_bullet_html.append(
                            """<li><a href="https://data.world/alleghenycounty/catalog/resource/{}">{}</a></li>""".
                            format(sub_bullets_list.iloc[sub]['encodedIri_y'],
                                   sub_bullets_list.iloc[sub]['title']))
                    sub_bullet_html = "".join(sub_bullet_html)
                    sub_bullet = """<ul style="padding-left: 30px;type: square;">{}</ul>""".format(sub_bullet_html)
                    row_html.append(
                        """<li><a href="https://data.world/alleghenycounty/catalog/resource/{}/columns">{}</a></li>{}""".
                        format(subcol_list_filter.loc[row]['encodedIri'],
                               subcol_list_filter.loc[row]['Datatable_Title_value'],
                               sub_bullet))
                    row_html = "".join(row_html)
                    remove_row = subcol_list_filter.loc[[row]]
                    anti_join = link_rows.merge(remove_row, how='outer', indicator=True)
                    anti_join = anti_join[anti_join['_merge'] == 'left_only']
                    link_rows = anti_join.drop(columns=['_merge'])
            else:
                row_html = ""
        else:
            row_html = ""
        link_list = ["""<li><a href="https://data.world/alleghenycounty/catalog/resource/{}/columns">{}</a></li>""".
                     format(link_rows['encodedIri'][row],
                            link_rows['Datatable_Title_value'][row]) for row in link_rows.index]
        link_list = "".join(link_list)
        final_list = """{}{}""".format(row_html, link_list)
        final_list = "".join(final_list)
        link_list = final_list
    else:
        link_list = ["""<li><a href="https://data.world/alleghenycounty/catalog/resource/{}/columns">{}</a></li>""".
                     format(link_rows['encodedIri'][row],
                            link_rows['Datatable_Title_value'][row]) for row in link_rows.index]
        link_list = "".join(link_list)

    message = template.format(link_list)
    return message


# Loops through every steward in stewards_table, converts specified tables/columns into links to data catalog,
# creates an email message based on an html template, then emails message to data steward.
for steward in stewards_table['DataSteward_value']:
    Email_Message = message_creater(steward, df_tables_n, EmailTemplate)
    Steward_Email = \
        stewards_table.loc[stewards_table['DataSteward_value'] == steward, 'DataSteward_EMAIL_value'].values[0]
    send_email(subject=email_subject, to_emails=Steward_Email,
               message=Email_Message)