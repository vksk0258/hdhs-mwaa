from airflow import DAG
import datetime
import pendulum

from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_template",
    # 이덱은 매일 0시 0분에 시작
    schedule="0 0 * * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 왠만하면 false
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["인프런","bash"]
) as dag:
    bash_t1 = BashOperator(
        task_id='bash_t1',
        bash_command='echo "data_interval_end: {{ data_interval_end }}"'
    )

    bash_t2 = BashOperator(
        task_id='bash_t2',
        env={
            'START_DATE':'{{data_interval_start | ds}}',
            'END_DATE':'{{data_interval_end | ds}}'
        },
        # &&가 붙으면 앞에 커맨드가 성공하면 뒤에있는 커맨드를 실행하겠다.
        bash_command='echo $START_DATE && echo $END_DATE'
    )

    bash_t1 >> bash_t2