from airflow import Dataset
from airflow import DAG
from airflow.operators.Bash import BashOperator
import pendulum

dataset_dags_dataset_producer_2 = Dataset("dags_dataset_prodcer_2")

with DAG(
    dag_id='dags_dataset_producer_2',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2026,1,6,tz='Asia/Seoul'),
    catchup=-False
) as dag:
    bash_task=BashOperator(
        task_id='bash_task',
        bash_command='echo "producer_2 수행 완료"'
    )