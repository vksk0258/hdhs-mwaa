from airflow import DAG
import datetime
import pendulum
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    # 이덱은 매월 아침 8시에 실행
    schedule="0 8 1 * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 왠만하면 false
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["인프런"]
) as dag:
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='vksk0258@naver.com',
        subject='Airflow 성공메일',
        html_content='Airflow 작업이 완료되었습니다'
    )
