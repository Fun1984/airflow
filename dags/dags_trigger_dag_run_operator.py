from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

with DAG(
    dag_id='dags_trigger_dag_run_operator',
    start_date=pendulum.datetime(2025,12,30,tz='Asia/Seoul'),
    schedule='30 9 * * *',
    catchup=False
) as dag:
    
    start_task = BashOperator(
        task_id = 'start_task',
        bash_command = 'echo "start!"',
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='dags_python_operator',
        trigger_run_id=None,
        # execution_date='{{data_interval_start}}', ##3.0에서는 삭제됨. 이상한 거 넣으면 에러 터짐. 
        # 강제로라도 하고 싶다면, conf={ "logical_date":"{{data_interval_end}}"} 로 쓰길 추천
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None
    )

    start_task >> trigger_dag_task