from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")
dataset_dags_dataset_producer_2 = Dataset("dags_dataset_producer_2")

with DAG(
    dag_id='dags_dataset_consumer_2',
    schedule=[dataset_dags_dataset_producer_1,dataset_dags_dataset_producer_2], #2개의 produce를 구독하겠다
    start_date=pendulum.datetime(2026,1,6,tz='Asia/Seoul'),
    catchup=-False
) as dag:
    bash_task=BashOperator(
        task_id='bash_task',
        bash_command='echo {{ ti.run_id }} && "producer_2이 완료되면 수행"'
    )