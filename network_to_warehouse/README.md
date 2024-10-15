# Network to Staging Templates

Templates for transferring files on county network drives to the CountyStat Data Warehouse

### Example Docker Operator
```python
from airflow.hooks.base import BaseHook
from docker.types import Mount
from datetime import timedelta

# BE SURE ABOVE IMPORTS INCLUDED IN DAG IMPORT LINES

wh_connection = BaseHook.get_connection("data_warehouse")

pull_FileOnNetwork = DockerOperator(
        task_id='pull_FileOnNetwork',
        image='countystats/network_to_warehouse:latest',
        api_version='1.39',
        auto_remove=True,
        execution_timeout=timedelta(minutes=5),
        environment={
            'DEPT': "DEPARTMENT NAME",
            'TABLE': 'TABLE NAME IN WAREHOUSE',
            'SOURCE': 'DATA SOURCE',
            'WH_HOST': wh_connection.host,
            'WH_DB': wh_connection.schema,
            'WH_USER': wh_connection.login,
            'WH_PASS': wh_connection.password,
            'SHEET': 'SOME SHEET NAME',
            'WORKBOOK': 'SomeExcelWorkbook.xlsx',
            'FILEPATH': './SOME_TARGET_DIRECTORY_IN_CONTAINER', # File path within mounted drive to directory, match path specified in target in below mount
            'COLTYPES': "date,numeric,date,numeric,text,text,text,text,text" # Comma Seperated List
        },
        docker_url='unix://var/run/docker.sock',
        command='bash network_to_warehouse.sh ',  # MUST HAVE SPACE AT END OF COMMAND, MUST I TELL YOU
        network_mode="bridge",
        mounts=[
            Mount(
                source='/home/mgradmin/Kerberos',
                target='/Kerberos',
                type='bind'
            ),
            Mount(
                source='/media/SOME_FOLDER_NAME',
                target='/SOME_TARGET_DIRECTORY_IN_CONTAINER', # Matches FILEPATH
                type='bind'
            )
        ]
    )
```

### Current Templates:
* [excel_to_staging.R](excel_to_staging.R): Transforms an Excel sheet into a table within the Staging schema of CountyStat Warehouse
* [warehouse_to_network.R](warehouse_to_network.R): Exports warehouse table to network drive
  * Parameters, **BOLD ARE REQUIRED**:
    * **WH_HOST** = Source database host
    * **WH_DB** = Source database
    * **WH_USER** = Source database username
    * **WH_PASS** = Source database password
    * **TABLE** = Full table name from source database to export
    * SCHEMA = Table's schema within source database (DEFAULT: Master)
    * **FILEPATH** = Folder path to destination drive/subfolder (R-readable filepath)
    * FILENAME = Filename for exported file within destination drive/folder (FILEPATH): defaults to name specified in 'TABLE' in none provided.
    * FILEEXT = Desired file extension for export, either 'csv' or 'excel' (DEFAULT: csv)
    * SHEETNAME = Desired sheet name within exported excel sheet (DEFAULT: Sheet1)
    * OVERWRITE = Have export overwrite files in destination drive/folder, either "TRUE" or "FALSE" with quotation marks (DEFAULT: "TRUE")