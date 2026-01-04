from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflwo.providers.postgres.hooks.postgres import PostgresHook


with DAG (
       dag_id='dags_python_with_postgres_hook_bulk_load',
       start_date=pendulum.datetime(2026,1,3,tz='Asia/Seoul'),
       schedule='0 7 * * *',
       catchup=False
) as dag :
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(tbl_nm, file_nm)
    
    insrt_postgres = PythoOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom',
                   'tbl_nm':'CardSubwayTime_bulk1',
                   'file_nm':'/opt/airflow/files/CardSubwayTime/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/CardSubwayTime.csv'}
    )