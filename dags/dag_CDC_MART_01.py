from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="dag_CDC_MART_01",
    schedule_interval=None,
    tags=["현대홈쇼핑", "DD01_0010_DAILY_MAIN"]
) as dag:
    trigger_dag_CDC_MART_LEV_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_LEV_01',
        trigger_dag_id='dag_CDC_MART_LEV_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_LEV_02 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_LEV_02',
        trigger_dag_id='dag_CDC_MART_LEV_02',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_ON_DEMAND_03 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_ON_DEMAND_03',
        trigger_dag_id='dag_CDC_MART_ON_DEMAND_03',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_MANG_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_MANG_01',
        trigger_dag_id='dag_CDC_MART_MANG_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_LEV_03 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_LEV_03',
        trigger_dag_id='dag_CDC_MART_LEV_03',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_LEV_04 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_LEV_04',
        trigger_dag_id='dag_CDC_MART_LEV_04',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_LEV_05 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_LEV_05',
        trigger_dag_id='dag_CDC_MART_LEV_05',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_CAMP = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_CAMP',
        trigger_dag_id='dag_CDC_MART_CAMP',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_DS_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_DS_01',
        trigger_dag_id='dag_CDC_MART_DS_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_DS_02 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_DS_02',
        trigger_dag_id='dag_CDC_MART_DS_02',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_PGM_REAL_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_PGM_REAL_01',
        trigger_dag_id='dag_CDC_MART_PGM_REAL_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_PGM_REAL_DATV_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_PGM_REAL_DATV_01',
        trigger_dag_id='dag_CDC_MART_PGM_REAL_DATV_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    # LEV_01 실행 후 병렬 작업
    trigger_dag_CDC_MART_LEV_01 >> [trigger_dag_CDC_MART_LEV_02, trigger_dag_CDC_MART_ON_DEMAND_03, trigger_dag_CDC_MART_MANG_01]

    trigger_dag_CDC_MART_LEV_02 >> trigger_dag_CDC_MART_LEV_03 >> trigger_dag_CDC_MART_LEV_04

    trigger_dag_CDC_MART_LEV_04 >> [trigger_dag_CDC_MART_DS_01, trigger_dag_CDC_MART_LEV_05]

    trigger_dag_CDC_MART_DS_01 >> trigger_dag_CDC_MART_DS_02

    trigger_dag_CDC_MART_LEV_05 >> trigger_dag_CDC_MART_CAMP

    trigger_dag_CDC_MART_MANG_01 >> trigger_dag_CDC_MART_PGM_REAL_01 >> trigger_dag_CDC_MART_PGM_REAL_DATV_01
