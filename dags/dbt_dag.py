import os
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import InvocationMode

from utils.pre_check import fetch_pandas_old

profile_config = ProfileConfig( 
    profile_name="cricket_analytics",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "cricketanalytics", "schema": "tranformed_data"},
    )
)


@dag(
    schedule_interval="*/10 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def main_dag():

    # Define Custom Task (Pre-check)
    custom_task = PythonOperator(
        task_id="snowflake_pre_check",
        python_callable=fetch_pandas_old,
    )

    # Define DBT Task Group (without dag=dag)
    dbt_snowflake_dag = DbtTaskGroup(
        group_id='dbt_task',
        project_config=ProjectConfig("/usr/local/airflow/dags/dbt/data_pipeline"),
        operator_args={"install_deps": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
            invocation_mode=InvocationMode.SUBPROCESS
        ),
        render_config=RenderConfig(
            enable_mock_profile=False,  # This is necessary to benefit from partial parsing when using ProfileMapping
        ),
    )

    # Ensure the custom task runs before DBT
    custom_task >> dbt_snowflake_dag

main_dag()
