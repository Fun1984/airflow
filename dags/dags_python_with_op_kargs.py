from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import regist2

with DAG(
    dag_id = "dags_python_with_op_kargs",
    schedule = "30 6 * * * ",
    start_date = pendulum.datetime(2025,12,18, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    regist_t1 = PythonOperator(
        task_id = 'regist_t1',
        python_callable = regist2,
        op_args=['cksdnd', 'man', 'kr', 'seoul'],
        op_kwargs={'email':'sorkejso@naver.com', 'phone':'010-1111-2223'}
    )

    regist_t1