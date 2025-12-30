from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.providers.http.operators.http import HttpOperator #SimpleHttpOperator(2.0) -> HttpOperator(3.0)
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator #plugins 밑으로 다 인식하고 있음
# from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dags_seoul_api_subway_custom',
    start_date=pendulum.datetime(2025,12,30,tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False,
) as dag:
    
    tb_subway_status = SeoulApiToCsvOperator(
        task_id='tb_subway_status',
        dataset_nm = 'CardSubwayTime', #이건 API 확인해서 넣어 줘야함.
        path='/opt/airflow/files/TbSubwayStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TbSubwayStatus.csv'
    )
    ## 강의는 이것말고 추가로 SeoulApiToCsvOperator를 사용해서 다른 걸 구함. 

    tb_subway_status
