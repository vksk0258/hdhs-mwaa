from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pandas as pd
import boto3
import json


with DAG(
    dag_id="dag_CDC_MART_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","DD01_0010_DAILY_MAIN"]
) as dag:
    trigger_dag_CDC_MART_LEV_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_LEV_01',
        trigger_dag_id='dag_CDC_MART_LEV_01',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_LEV_02 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_LEV_02',
        trigger_dag_id='dag_CDC_MART_LEV_02',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_ON_DEMAND_03 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_ON_DEMAND_03',
        trigger_dag_id='dag_CDC_MART_ON_DEMAND_03',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_MANG_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_MANG_01',
        trigger_dag_id='dag_CDC_MART_MANG_01',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_LEV_03 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_LEV_03',
        trigger_dag_id='dag_CDC_MART_LEV_03',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_LEV_04 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_LEV_04',
        trigger_dag_id='dag_CDC_MART_LEV_04',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_LEV_05 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_LEV_05',
        trigger_dag_id='dag_CDC_MART_LEV_05',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_CAMP = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_CAMP',
        trigger_dag_id='dag_CDC_MART_CAMP',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_DS_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_DS_01',
        trigger_dag_id='dag_CDC_MART_DS_01',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_DS_02 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_DS_02',
        trigger_dag_id='dag_CDC_MART_DS_02',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_PGM_REAL_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_PGM_REAL_01',
        trigger_dag_id='dag_CDC_MART_PGM_REAL_01',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_PGM_REAL_DATV_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_PGM_REAL_DATV_01',
        trigger_dag_id='dag_CDC_MART_PGM_REAL_DATV_01',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    # LEV_01 실행 후 병렬 작업
    trigger_dag_CDC_MART_LEV_01 >> [trigger_dag_CDC_MART_LEV_02, trigger_dag_CDC_MART_ON_DEMAND_03, trigger_dag_CDC_MART_MANG_01]

    trigger_dag_CDC_MART_LEV_02 >> trigger_dag_CDC_MART_LEV_03 >> trigger_dag_CDC_MART_LEV_04

    trigger_dag_CDC_MART_LEV_04 >> [trigger_dag_CDC_MART_DS_01, trigger_dag_CDC_MART_LEV_05]

    trigger_dag_CDC_MART_DS_01 >> trigger_dag_CDC_MART_DS_02

    trigger_dag_CDC_MART_LEV_05 >> trigger_dag_CDC_MART_CAMP

    trigger_dag_CDC_MART_MANG_01 >> trigger_dag_CDC_MART_PGM_REAL_01 >> trigger_dag_CDC_MART_PGM_REAL_DATV_01
