# tableau-transfer

This script will take data from the Datawarehouse and upload it to the Tableau Server.

Image Name: `countystats/tableau-transfer:python`

## Enviornmental Variables:
* dept: Schema and Department Name*
* table: Datawarehouse table name*
* schema: Datawarehouse Schema to pull from
  * Default: `Reporting`
* name: Data Source name for tableau server*
* mode: Data source write mode*
  * Default value: `Overwrite` 
  * Valid values: `CreateNew, Overwrite, Append`
* project_id: Tableau Server Project ID
* project_name: Tableau Server Project Name (Required if project_id is not supplied)
  * Default: dept enviornmental variable
* site: Tableau Server Site
  * Default value: `CountyStats`
* tableau_username: Tableau Server Airflow Variable*
* tableau_password: Tableau Server Airflow Variable*
* wh_host: Airflow Connection value*
* wh_db: Airflow Connection value*
* wh_un: Airflow Connection value*
* wh_pw: Airflow Connection value*

(*) Required variable

## Dag Example

```
wh_connection = BaseHook.get_connection("data_warehouse")
dept = 'Example_Dept'
...
tableau_demog = DockerOperator(
                task_id='tableau_demog',
                image='countystats/tableau-transfer:python',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'name': 'Example',
                    'dept': dept,
                    'table': 'Example',
                    'ts_username': Variable.get("tableau_username"),
                    'ts_password': Variable.get("tableau_password"),
                    'wh_host': wh_connection.host,
                    'wh_db': wh_connection.schema,
                    'wh_user': wh_connection.login,
                    'wh_pass': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                command='python3 Tableau-Transfer.py',
                network_mode="bridge"
        )
```