from airflow import DAG
import datetime
import pendulum

from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_conn_test",
    # 이덱은 매일 0시 0분에 시작
    schedule="0 0 * * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 왠만하면 false
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["인프런"]
) as dag:

    t1 = EmptyOperator(
        task_id='t1'
    )

    t2 = EmptyOperator(
        task_id='t2'
    )

    t3 = EmptyOperator(
        task_id='t3'
    )

    t4 = EmptyOperator(
        task_id='t4'
    )

    t5 = EmptyOperator(
        task_id='t5'
    )

    t6 = EmptyOperator(
        task_id='t6'
    )

    t7 = EmptyOperator(
        task_id='t7'
    )

    t8 = EmptyOperator(
        task_id='t8'
    )

    t1 >> [t2,t3] >> t4
    t5 >> t4
    [t4, t7] >> t6 >> t8