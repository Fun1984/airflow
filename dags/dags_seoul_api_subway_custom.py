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
        #태스크 수행 주체는 워커컨테이너임. wsl컴퓨터와 연결되어 있지 않음. docker-compose.yaml에 연결해줘야함
        #vim으로 들어간 후, x-airflow-common: volumes:에서 - ${AIRFLOW_PROJ_DIR:-.}/airflow/files:/opt/airflow/files를 입력해주고
        # 그 후, (연결설정이 된)airflow 폴더에서 files를 mkdir 해준다. 
        task_id='tb_subway_status',
        dataset_nm = 'CardSubwayTime', #이건 API 확인해서 넣어 줘야함.
        path='/opt/airflow/files/TbSubwayStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}', #이 부분은 wsl이 아니라, 실행되는 워커컨테이너 내부임.
        file_name='TbSubwayStatus.csv'
    )
    ## 강의는 이것말고 추가로 SeoulApiToCsvOperator를 사용해서 다른 걸 구함. 

    tb_subway_status
