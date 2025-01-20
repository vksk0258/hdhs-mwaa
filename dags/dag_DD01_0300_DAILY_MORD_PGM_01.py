from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pandas as pd
import boto3
import json


with DAG(
    dag_id="dag_DD01_0300_DAILY_MORD_PGM_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","100_COM"]
) as dag:
    trigger_dag_CDC_MART_MORD_PGM_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_MORD_PGM_01',
        trigger_dag_id='dag_CDC_MART_MORD_PGM_01',
        trigger_run_id=None,
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_MORD_PGM_01

