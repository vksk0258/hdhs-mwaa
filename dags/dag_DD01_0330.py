from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pandas as pd
import boto3
import json


with DAG(
    dag_id="dag_DD01_0330",
    schedule_interval=None,
    tags=["현대홈쇼핑","100_COM"]
) as dag:
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)


    python_task_1 = print_context('task_decorator 실행')