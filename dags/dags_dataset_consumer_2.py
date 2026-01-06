import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow.operators.bash import BashOperator
#from airflow import DAG
#from airflow import Dataset

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