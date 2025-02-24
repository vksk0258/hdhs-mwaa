from airflow import DAG
from operators.oracle_to_snowflake_initial_load_operator import OracleToSnowflakeInitialLoadOperator
import datetime
import pendulum


with DAG(
    dag_id="dag_CDC_ODS_02",
    schedule_interval='30 0 * * *',
    start_date=pendulum.datetime(2025, 2, 23, tz="Asia/Seoul"),
    tags=["현대홈쇼핑","DD01_0010_DAILY_MAIN"]
) as dag:
    task_TMS_CAMP_SEND_LIST_01 = OracleToSnowflakeInitialLoadOperator(
        task_id = "task_TMS_CAMP_SEND_LIST_01",
        oracle_conn_id = "conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table = "ODS_TMS.TMS_CAMP_SEND_LIST_01",
        snowflake_table = "ODS_TMS.TMS_CAMP_SEND_LIST_01",
        columns = ['*'],
        batch_size = 200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_SEND_LIST_02 = OracleToSnowflakeInitialLoadOperator(
        task_id="task_TMS_CAMP_SEND_LIST_02",
        oracle_conn_id="conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table="ODS_TMS.TMS_CAMP_SEND_LIST_02",
        snowflake_table="ODS_TMS.TMS_CAMP_SEND_LIST_02",
        columns=['*'],
        batch_size=200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_SEND_LIST_03 = OracleToSnowflakeInitialLoadOperator(
        task_id="task_TMS_CAMP_SEND_LIST_03",
        oracle_conn_id="conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table="ODS_TMS.TMS_CAMP_SEND_LIST_03",
        snowflake_table="ODS_TMS.TMS_CAMP_SEND_LIST_03",
        columns=['*'],
        batch_size=200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_SEND_LIST_04 = OracleToSnowflakeInitialLoadOperator(
        task_id="task_TMS_CAMP_SEND_LIST_04",
        oracle_conn_id="conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table="ODS_TMS.TMS_CAMP_SEND_LIST_04",
        snowflake_table="ODS_TMS.TMS_CAMP_SEND_LIST_04",
        columns=['*'],
        batch_size=200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_SEND_LIST_05 = OracleToSnowflakeInitialLoadOperator(
        task_id="task_TMS_CAMP_SEND_LIST_05",
        oracle_conn_id="conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table="ODS_TMS.TMS_CAMP_SEND_LIST_05",
        snowflake_table="ODS_TMS.TMS_CAMP_SEND_LIST_05",
        columns=['*'],
        batch_size=200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_SEND_LIST_06 = OracleToSnowflakeInitialLoadOperator(
        task_id="task_TMS_CAMP_SEND_LIST_06",
        oracle_conn_id="conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table="ODS_TMS.TMS_CAMP_SEND_LIST_06",
        snowflake_table="ODS_TMS.TMS_CAMP_SEND_LIST_06",
        columns=['*'],
        batch_size=200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_SEND_LIST_07 = OracleToSnowflakeInitialLoadOperator(
        task_id="task_TMS_CAMP_SEND_LIST_07",
        oracle_conn_id="conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table="ODS_TMS.TMS_CAMP_SEND_LIST_07",
        snowflake_table="ODS_TMS.TMS_CAMP_SEND_LIST_07",
        columns=['*'],
        batch_size=200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_SEND_LIST_08 = OracleToSnowflakeInitialLoadOperator(
        task_id="task_TMS_CAMP_SEND_LIST_08",
        oracle_conn_id="conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table="ODS_TMS.TMS_CAMP_SEND_LIST_08",
        snowflake_table="ODS_TMS.TMS_CAMP_SEND_LIST_08",
        columns=['*'],
        batch_size=200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_SEND_LIST_09 = OracleToSnowflakeInitialLoadOperator(
        task_id="task_TMS_CAMP_SEND_LIST_09",
        oracle_conn_id="conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table="ODS_TMS.TMS_CAMP_SEND_LIST_09",
        snowflake_table="ODS_TMS.TMS_CAMP_SEND_LIST_09",
        columns=['*'],
        batch_size=200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_SEND_LIST_10 = OracleToSnowflakeInitialLoadOperator(
        task_id="task_TMS_CAMP_SEND_LIST_10",
        oracle_conn_id="conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table="ODS_TMS.TMS_CAMP_SEND_LIST_10",
        snowflake_table="ODS_TMS.TMS_CAMP_SEND_LIST_10",
        columns=['*'],
        batch_size=200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_SEND_LIST_11 = OracleToSnowflakeInitialLoadOperator(
        task_id="task_TMS_CAMP_SEND_LIST_11",
        oracle_conn_id="conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table="ODS_TMS.TMS_CAMP_SEND_LIST_11",
        snowflake_table="ODS_TMS.TMS_CAMP_SEND_LIST_11",
        columns=['*'],
        batch_size=200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_SEND_LIST_12 = OracleToSnowflakeInitialLoadOperator(
        task_id="task_TMS_CAMP_SEND_LIST_12",
        oracle_conn_id="conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table="ODS_TMS.TMS_CAMP_SEND_LIST_12",
        snowflake_table="ODS_TMS.TMS_CAMP_SEND_LIST_12",
        columns=['*'],
        batch_size=200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_SEND_LIST_01 >> \
    task_TMS_CAMP_SEND_LIST_02 >> \
    task_TMS_CAMP_SEND_LIST_03 >> \
    task_TMS_CAMP_SEND_LIST_04 >> \
    task_TMS_CAMP_SEND_LIST_05 >> \
    task_TMS_CAMP_SEND_LIST_06 >> \
    task_TMS_CAMP_SEND_LIST_07 >> \
    task_TMS_CAMP_SEND_LIST_08 >> \
    task_TMS_CAMP_SEND_LIST_09 >> \
    task_TMS_CAMP_SEND_LIST_10 >> \
    task_TMS_CAMP_SEND_LIST_11 >> \
    task_TMS_CAMP_SEND_LIST_12
