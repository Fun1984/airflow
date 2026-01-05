import pendulum

from airflow.sdk import DAG
from airflow.sdk.bases.operator import BashOperator
from airflow.providers.standard.sensors.bash import BashSensor

# Airflow 2.10.5 이하 버전
#from airflow.operators.bash import BashOperator
#from airflow.sensors.bash import BashSensor
#from airflow import DAG

with DAG( 
    dag_id = "dags_bash_sensor",
    schedule = "0 6 * * *",
    start_date = pendulum.datetime(2026, 1, 3, tz="Asia/Seoul"),
    catchup=False
) as dag:

    sensor_task_by_poke = BashSensor(
        task_id='sensor_task_by_poke',
        env={'FILE':'/opt/airflow/files/TbSubwayStatus/{{data_interval_start.in_timezone("Asia/Seoul") | ds_nodash}}/CardSubwayTime.csv'},
        bash_command=f'''echo $FILE &&
                        if [ -f $FILE ]; then
                            exit 0
                        else
                            exit 1
                        fi''',
        poke_interval=30, #30초
        timeout=60*2, #2분
        mode='poke',
        soft_fail=False
    )

    sensor_task_by_reschedule = BashSensor(
        task_id='sensor_task_by_reschedule',
        env={'FILE':'/opt/airflow/files/TbSubwayStatus/{{data_interval_start.in_timezone("Asia/Seoul") | ds_nodash}}/CardSubwayTime.csv'},
        bash_command=f'''echo $FILE &&
                        if [ -f $FILE ]; then
                            exit 0
                        else
                            exit 1
                        fi''',
        poke_interval=30*3, #3분
        timeout=60*9, #9분
        mode='reschedule',
        soft_fail=True
    )

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "건수: `cat $FILE | wc -l`"'
    )

    [sensor_task_by_poke, sensor_task_by_reschedule] >> bash_task