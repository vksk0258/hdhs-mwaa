from airflow import DAG
import datetime
import pendulum
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_python_with_xcom",
    # 이덱은 매일 6시 30분에 시작
    schedule="30 6 * * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["인프런","bash",'python'],
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 웬만하면 false
    catchup=False
) as dag:
    @task(task_id='python_push')
    def python_push_xcom():
        result_dict = {'status' : 'good', 'data' : [1,2,3], 'option_cnt' : 100}
        return result_dict

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={
            'status' : '{{ ti.xcom_pull(task_ids="python_push")["status"] }}',
            'data' : '{{ ti.xcom_pull(task_ids="python_push")["data"] }}',
            'option_cnt' : '{{ ti.xcom_pull(task_ids="python_push")["option_cnt"] }}'
        },
        bash_command='echo $status && echo $data && echo $option_cnt'
    )
    python_push_xcom() >> bash_pull

    bash_push = BashOperator(
        task_id='bash_push',
        bash_command='echo PUSH_START'
                     '{{ ti.xcom_push(key="bash_pushed", value=200) }} && '
                     'echo PUSH_END'
    )

    @task(task_id='python_pull')
    def python_pull_xcom(**kwargs):
        ti = kwargs['ti']
        status_value = ti.xcom_pull(key="bash_pushed")
        return_value = ti.xcom_pull(task_ids="bash_push")
        print('status_value', str(status_value))
        print('return_value', return_value)

    bash_pull >> python_pull_xcom()