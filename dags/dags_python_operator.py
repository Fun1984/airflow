from airflow import DAG
import pendulum #datetime이 있지만, 좀 더 쉽게 쓸수 있게 해주는 라이브러리
import datetime
from airflow.operators.python import PythonOperator

import random

with DAG(
    dag_id="dags_python_operator", #Airflow 웹 내 dag이름 #파이썬 파일 명이랑은 상관없다(하지만 일치시키는 게 좋다)
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 12, 17, tz="Asia/Seoul"),
    catchup=False, 
) as dag:
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0, 3)
        print(fruit[rand_int])
    
    py_t1 = PythonOperator(
        task_id = 'py_t1',
        python_callable=select_fruit
    )

    py_t1