from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_xcom",
    # 이덱은 매일 6시 30분에 시작
    schedule="30 6 * * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 웬만하면 false
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["인프런","bash",'xcom']
) as dag:
    bash_push = BashOperator(
        task_id="bash_push",
        bash_command="echo START && "
                     "echo XCOM_PUSHED "
                     "{{ ti.xcom_push(key='bash_pushed', value='first_bash_message') }} && "
                     "echo COMPLETE"
    )

    bash_pull = BashOperator(
        task_id="bash_pull",
        env={'PUSHED_VALUE':"{{ ti.xcom_pull(key='bash_pushed') }}",
             'RETURN_VALUE':"{{ ti.xcom_pull(task_ids='bash_push') }}"},
        bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE",
        do_xcom_push=False
    )

    bash_push >> bash_pull