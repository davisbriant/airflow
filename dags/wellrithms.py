from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.decorators import dag, task 
from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import AthenaAccessKeyProfileMapping
from datetime import date, datetime, time, timedelta
import os
from includes.utils import s3_utils
import yaml
import pendulum

config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config/', 'wellrithms.yaml')
with open(config_file_path) as config_file:
    config = yaml.safe_load(config_file)

ATHENA_CONN_ID = "aws_default"
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/includes/dbt/wellrithms"
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
S3_PATH = 'sources/test_data'
S3_FILE = 'test_data'
SOURCE_TABLE_NAME = 'sources-test_datajson'
DROP_TABLE_QUERY = f"""DROP TABLE IF EXISTS {config['athena']['schema']}.`{SOURCE_TABLE_NAME}`"""
CREATE_TABLE_QUERY = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {config['athena']['schema']}.`{SOURCE_TABLE_NAME}`(json_payload STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION 's3://wellrithms/athena/sources/test_data/json/'"""

default_args = {
    'owner': 'airflow'
    ,'depends_on_past': False
    ,'start_date': pendulum.today('UTC').add(days=-2)
    ,'email': config['alerting']['emails']
    ,'email_on_failure': True
    ,'email_on_retry': False
    ,'retries': 0
    ,'retry_delay': timedelta(minutes=5)
}
with DAG(dag_id='wellrithms', schedule="@hourly", catchup=False, default_args=default_args) as dag:

    @task(task_id="convert_csv_file_to_json")
    def convertFile():
        response = s3_utils.s3Utils(config).csvToJsonS3(S3_PATH, S3_FILE)
        return response

    drop_table = AthenaOperator(
        task_id = 'drop_source_table_if_exists',
        query = DROP_TABLE_QUERY,
        database = config['athena']['database'],
        output_location= config['athena']['s3_staging_dir'],
        sleep_time = 30,
    )

    create_table = AthenaOperator(
        task_id = 'create_source_table_if_not_exists',
        query = CREATE_TABLE_QUERY,
        database = config['athena']['database'],
        output_location= config['athena']['s3_staging_dir'],
        sleep_time = 30,
    )

    # Profile mapping for Athena
    profile_config = ProfileConfig(
        profile_name="wellrithms",
        target_name="dev",
        profile_mapping=AthenaAccessKeyProfileMapping(
            conn_id=ATHENA_CONN_ID,
            profile_args={
                "s3_staging_dir": config['athena']['s3_staging_dir'],
                "database": config['athena']['database'],
                "schema": config['athena']['schema'],
                "region_name": config['athena']['region'],
            },
        )
    )

    athena_dbt_tasks = DbtTaskGroup(
        group_id="athena_dbt_models",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            # execution_mode="VIRTUALENV",
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        operator_args={
            "install_deps": True,  # Run dbt deps before executing
        },
    )

    convert_file = convertFile()

    convert_file >> drop_table >> create_table >> athena_dbt_tasks
