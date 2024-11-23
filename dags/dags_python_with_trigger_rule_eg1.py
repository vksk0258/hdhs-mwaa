from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

with DAG(
    dag_id="dags_python_with_trigger_rule_eg1",
    # 이덱은 매일 6시 30분에 시작
    schedule="30 6 * * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 웬만하면 false
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["인프런",'python','트리거']
) as dag:
    bash_upstream_1 = BashOperator(
        task_id='bash_upstream_1',
        bash_command='echo upstream1'
    )

    @task(task_id='bash_upstream_1')
    def python_upstream_1():
        raise AirflowException('Upstream task failed')

    @task(task_id='bash_upstream_2')
    def python_upstream_2():
        print('정상처리')

    @task(task_id='python_downstream_1',trigger_rule='all_done')
    def python_downstream_1():
        print('정상처리')

    [bash_upstream_1,python_upstream_1(), python_upstream_2()] >> python_downstream_1()