from sensors.seoul_api_date_sensor import SeoulApiDateSensor
import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG

with DAG(
    dag_id='dags_custom_sensor',
    start_date=pendulum.datetime(2025,1,5, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    CardSubwayTime_sensor = SeoulApiDateSensor(
        task_id='CardSubwayTime_sensor',
        dataset_nm='CardSubwayTime',
        base_dt_col='JOB_YMD',
        searched_ym='202512',
        day_off=-30,
        poke_interval=600,
        mode='reschedule'
    )
    
