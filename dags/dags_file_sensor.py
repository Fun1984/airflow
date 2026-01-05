import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG

with DAG(
    dag_id='dags_file_sensor',
    start_date=pendulum.datetime(2026,1,3,tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False
) as dag:

    TbSubwayStatus_sensor = FileSensor(
        task_id='TbSubwayStatus_sensor',
        fs_conn_id='conn_file_opt_airflow_files',
        filepath='TbSubwayStatus/{{data_interval_start.in_timezone("Asia/Seoul") | ds_nodash }}/CardSubwayTime.csv',
        recursive=False,
        poke_interval=60,
        timeout=60*60*24, #1일
        mode='reschedule'
    )

    TbSubwayStatus_sensor