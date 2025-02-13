# oracle-to-staging

Provide a list of tables within a database schema and move them to the Datawarehouse from an Oracle Database. 

Image name: `countystats/oracle-to-staging:r`

## Enviornmental Variables
* DEPT: Department Name*
* TABLES: Comma seperated list of tables to be pulled from Data source*
* SQL: Custom SQL code for pulling out a table.
  * Note: if you are using this feature then you should only pass one table name in the `TABLES` field.
* USER: Source Database login Connection value *
* PASSWORD: Source Database password Connection value*
* HOST: Source Database host Connection value*
* PORT: Source Database port Connection value
  * Default `1521`
* DATABASE: Source Database schema Connection value*
* WH_HOST: Data Warehouse login Connection value*
* WH_DB: Data Warehouse database Connection value*
* WH_UN: Data Warehouse login Connection value*
* WH_PW: Data Warehouse password Connection value*
* APPEND_COL: Column to check for new values
* APPEND_TYPE: SQL Function to use to find new values
  * Default value `MAX`
* APPEND_SIGN: WHERE statement sign for appending new data.
  * Default: `>`
* MAX_COLS: Comma separated string of columns which will need to have a `varchar(max)` setting to avoid truncation.

*Required Variable

## Example Dag Usage:
```
connection = BaseHook.get_connection("airflow_connection_id") # This is pulled from Admin > connections
wh_connection = BaseHook.get_connection("data_warehouse")
...
table_pull = DockerOperator(
                task_id='table_pull',
                image='countystats/oracle-to-staging:r',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'DEPT': 'Department_Name_and/or_Warehouse_Schema',
                    'TABLES': 'Name,Of,Tables,Comma,Separated',
                    'USER': connection.login,
                    'PASS': connection.password,
                    'HOST': connection.host,
                    'PORT': connection.port,
                    'DATABASE': connection.schema,
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'WH_USER': wh_connection.login,
                    'WH_PASS': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                command='Rscript oracle_to_staging.R',
                network_mode="bridge"
        )
```
