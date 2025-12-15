import pendulum #datetime이 있지만, 좀 더 쉽게 쓸수 있게 해주는 라이브러리
import datetime
from airflow.sdk import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="example_bash_operator", #Airflow 웹 내 dag이름 #파이썬 파일 명이랑은 상관없다(하지만 일치시키는 게 좋다)
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False, #False - start_date와 현시간 간 누락 기간까지 돌리지 않는다 | True - (한꺼번에) 돌린다 <가급적 False로 두는 게 좋다>  
    # dagrun_timeout=datetime.timedelta(minutes=60), #<- 몇 분 지나면은 타임아웃 
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:
    bash_t1 = BashOperator( #객체명과 task명은 일치시켜두면 안 했갈린다
        task_id = "bash_t1",
        bash_command = "echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2
