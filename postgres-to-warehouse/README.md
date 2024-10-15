# postgres-to-warehouse

Container used to import data from PostgreSQL to CountyStat warhoue

**Image name**: `countystats/postgres-to-warehouse:1.0`

**Tag/Versions**:
* _1.0_

## Enviornmental Variables
* DEPT: Department Name*
* TABLES: Comma seperated list of tables to be pulled from Data source*
* SQL: Custom SQL code for pulling out a table.
  * _Note_: if you are using this feature then you should only pass one table name in the `TABLES` field.
* _Source Database Connection Config_:
  * PG_USER: Source Database login Connection value *
  * PG_PASS: Source Database password Connection value*
  * PG_HOST: Source Database host Connection value*
  * PG_DB: Source Database Connection value*
  * PG_PORT: Source Database Port value*
  * SCHEMA: Source Schema value
    * Default value `dbo`
* _Warehouse Connection Config_:
  * WH_HOST: Data Warehouse login Connection value*
  * WH_DB: Data Warehouse database Connection value*
  * WH_UN: Data Warehouse login Connection value*
  * WH_PW: Data Warehouse password Connection value*
  * WH_SCHEMA: Data Warehouse destination schema
    * Default value `Staging`
* _Append Methods_:
  * APPEND_COL: Column to check for new values
  * APPEND_TYPE: SQL Function to use to find new values
    * Default value `MAX`
  * APPEND_SIGN: WHERE statement sign for appending new data.
    * Default: `>`
* MAX_COLS: Comma separated string of columns which will need to have a `varchar(max)` setting to avoid truncation.

*Required Variable

### DAG Example:
```
postgres_import = DockerOperator(
                task_id='postgres_import',
                image='countystats/postgres-to-warehouse:1.0',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'DEPT': 'Department_Name_and/or_Warehouse_Schema',
                    'TABLES': 'Name,Of,Tables,Comma,Separated',
                    'PG_USER': connection.login,
                    'PG_PASS': connection.password,
                    'PG_HOST': connection.host,
                    'PG_PORT': connection.port,
                    'PG_DB': connection.schema,
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'WH_USER': wh_connection.login,
                    'WH_PASS': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                network_mode="bridge"
        )
```
