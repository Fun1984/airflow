from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.branch import BaseBranchOperator ##일단 된다.(Document에선 최신 버전에서 다른 부분을 가리킴)
from airflow.exceptions import AirflowException

with DAG(
    dag_id='dags_python_with_trigger_rule_eg1',
    start_date=pendulum.datetime(2025,12,28,tz="Asia/Seoul"),
    schedule=None,
    catchup=False
) as dag:
    bash_upstream_1 = BashOperator(
        task_id='bash_upstream_1',
        bash_command='echo upstream1'
    )

    @task(task_id='python_upstream_1')
    def python_upstream_1():
        import random
        if random.choise([0,1]) == 0 :
            raise AirflowException('downstream_1 Exception!')
        else : 
            print('정상 처리')

    @task(task_id='python_upstream_2')
    def python_upstream_2():
        print('정상 처리')
    
    @task(task_id='python_downstream_1', trigger_rule='all_done')
    def python_downstream_1():
        print('정상 처리')
    
    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()