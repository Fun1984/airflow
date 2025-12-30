from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import HttpOperator #SimpleHttpOperator(2.0) -> HttpOperator(3.0)
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2025,12,30,tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    tb_cycle_station_info = SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/CardSubwayTime/1/5/202511', #보안리스크로 apikey를 넣는 것은 비추천함. airflow에 variables를 때리는 게 좋다. #{{var.value.apikey_openapi_seoul_go_kr}} 
        method='GET',
        headers={'Content-Type':'application/json',
                 'charset':'utf-8',
                 'Accept':'*/*'}
    )
    
    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info') #_info의 리턴값을 가져오는 것
        import json
        from pprint import pprint
        pprint(json.loads(rslt))

    
    tb_cycle_station_info >> python_2()
