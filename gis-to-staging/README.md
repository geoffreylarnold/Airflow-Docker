#  gis-to-staging
Provide an Allegheny County Esri Service to add to the DataWarehouse.

Image name: `countystats/gis-to-staging:r`

## Enviornmental Variables
* DEPT: Department Name*
* TABLE: Table Name you want for the FeatureServer being targeted*
* LOGIN: ArcGIS Online Login*
* PASSWORD: ArcGIS Online Password*
* WH_HOST: Data Warehouse login Connection value*
* WH_DB: Data Warehouse database Connection value*
* WH_UN: Data Warehouse login Connection value*
* WH_PW: Data Warehouse password Connection value*
* SERVICE: Full URL Link to feature service*
* UPDATE_COL: Column to check for new records based on source location.
* OFFSET: Number of records to be read per chunk (do not make this more than the services `Max Record Count` indicated in the API page.
  * Default: 1000
* INT_COL: Specify columns with sparse data that need to maintain integer formatting, avoids mismatch error when employing offset. 

*Required Variable

## Example Dag Usage:
```
wh_connection = BaseHook.get_connection("data_warehouse")
...
pull_permits = DockerOperator(
                task_id='pull_permits',
                image='countystats/gis-to-staging:r',
                api_version='1.39',
                auto_remove=True,
                execution_timeout=timedelta(minutes=30),
                environment={
                    'PASSWORD': Variable.get('arcgis_online_password'),
                    'LOGIN': Variable.get('arcgis_online_uid'),
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'WH_USER': wh_connection.login,
                    'WH_PASS': wh_connection.password,
                    'DEPT': dept,
                    'SERVICE': 'https://services1.arcgis.com/vdNDkVykv9vEWFX4/ArcGIS/rest/services/DPW_Permits_V4/FeatureServer/0/',
                    'TABLE': 'Permits',
                    'UPDATE_COL': 'last_edited_date'
                },
                docker_url='unix://var/run/docker.sock',
                network_mode="bridge"
        )
```
