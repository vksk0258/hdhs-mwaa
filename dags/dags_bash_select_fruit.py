from airflow import DAG
import datetime
import pendulum

from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    # 매월 첫번째 주 토요일 0시 10분 실행
    schedule="10 0 * * 6#1",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 웬만하면 false
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["인프런","bash"]
) as dag:

    t1_APPLE = BashOperator(
        task_id='t1_APPLE',
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh APPLE"
    )

    t2_GRAPE = BashOperator(
        task_id='t2_GRAPE',
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh GRAPE"
    )

    t1_APPLE >> t2_GRAPE