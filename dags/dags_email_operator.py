from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG( 
    dag_id = "dags_email_operator",
    schedule = "0 8 1 * *", #매월 1일날 아침 8시 작업 
    start_date = pendulum.datetime(2025, 12, 17, tz="Asia/Seoul"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id='send_email_task',
        conn_id='my-email-conn', #airflow 3.0 이후 추가 필요
        to='sorkejso@naver.com',
        subject='Airflow 성공메일',
        html_content='Airflow 작업이 완료되었습니다'
    )