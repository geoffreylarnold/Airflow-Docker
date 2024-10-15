# data-validate

This container is the trickiest. First, in order to verify a schema you have to create a schema json document and upload it into Airflow. The steps for creating this document are below this are below.

Image name: `countystats/data-validate:r`

# Generate Schema Variable

### Step 0: Put the Data into Staging!

The `generate-schema.R` script is expecting the Table to exist in the `Staging` schema. The easiest way to do this is to run only the data import Tasks from your DAG on Airflow. Since you should only be doing this step on the Development server, this won't matter.

### Step 1: Create env file

In your favorite text editor create a `.env` file in the working directory of your R session with the format in `example_dot_env`. You will need to do this for each table you wish to validate.

### Step 2: Run generate-schema.R and edit output.

Run generate-schema.R in R Studio, at the end the file will open up for editing. The script will set NAs to false for any column names in the `NAS_FALSE` env variable.

### Step 3: Upload Schema as a Variable to Airflow

In Airflow complete the following:

* Click Admin
* Select Variables
* Click the Plus Icon (+) to add a new variable
* Copy and paste the printed schema name into the Key input
* Copy and paste the contents of the JSON file to the Value input

Now your schema is ready for use in a DAG!

## DAG Enviornmental Variables

* DEPT: Department name*
* TABLE: Source table name*
* COL_SCHEMA: Airfow variable name (`Variable.get("schema_variable_name"`)*
* SOURCE: Source database, schema, system or API*
* COLS: Order for column names to be queried in order to avoid `Invalid Descriptor Index` error
  * Example: `COLS: "id, name, [date]"`
* WH_HOST: Data Warehouse login Connection value*
* WH_DB: Data Warehouse database Connection value*
* WH_UN: Data Warehouse login Connection value*
* WH_PW: Data Warehouse password Connection value*

*Required variable

### DAG Example:
```
schema = schema_device = DockerOperator(
                task_id='schema_validate',
                image='countystats/data-validate:r',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'DEPT': 'Example Dept',
                    'TABLE': 'some_table',
                    'COL_SCHEMA': Variable.get("schema_variable_name"),
                    'SOURCE': connection.schema,
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'WH_USER': wh_connection.login,
                    'WH_PASS': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                command='Rscript schema-validate.R',
                network_mode="bridge"
        )
```

### Iteration example

```python
from airflow.decorators import task_group

def clean_list(oldlist):
      cleaned = oldlist.__str__().replace("[", "").replace("'", "").replace("]", "").replace(" ", "")
      return cleaned
    
tables_l = ['List', 'Of', 'Tables']
tables = clean_list(tables_l)    
    # ------- 
    
    def run_dag_task(variable):
        validate_schema = DockerOperator(
            task_id=f'Validate_{variable}',
            image='countystats/data-validate:r',
            api_version='1.39',
            auto_remove=True,
            environment={
                'DEPT': dept,
                'TABLE': variable,
                'COL_SCHEMA': Variable.get("""{}_schema""".format(variable)),
                'SOURCE': connection.schema,
                'WH_HOST': wh_connection.host,
                'WH_DB': wh_connection.schema,
                'WH_USER': wh_connection.login,
                'WH_PASS': wh_connection.password
            },
            docker_url='unix://var/run/docker.sock',
            command='Rscript schema-validate.R',
            network_mode="bridge"
        )
        return validate_schema


    @task_group(group_id='data_validations')
    def tg1():
        for tables_l in full_tables[0:]:
            task_iterate = run_dag_task(tables_l)
            task_iterate
```