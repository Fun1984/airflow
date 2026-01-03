from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
# from airflow.providers.http.operators.http import HttpOperator #SimpleHttpOperator(2.0) -> HttpOperator(3.0)
# from airflow.decorators import task

with DAG(
       dag_id='dags_python_with_postgres_HOOK',
       start_date=pendulum.datetime(2026,1,3,tz='Asia/Seoul'),
       schedule=None,
       catchup=False
) as dag : 
       def insrt_postgres(postgres_conn_id, **kwargs) :
              # import psycopg2
              from airflow.providers.postgres.hooks.postgres import PostgresHook
              from contextlib import closing

              postgres_hook = PostgresHook(postgres_conn_id)
              with closing(postgres_hook.get_conn()) as conn:
                     with closing(conn.cursor()) as cursor:
                            dag_id = kwargs.get('ti').dag_id
                            task_id = kwargs.get('ti').task_id
                            run_id = kwargs.get('ti').run_id
                            msg='insrt 수행'
                            sql='insert into py_opr_drct_insrt values (%s,%s,%s,%s);'
                            cursor.execute(sql, (dag_id, task_id, run_id, msg))
                            conn.commit()

       insrt_postgres_with_hook = PythonOperator(
              task_id='insrt_postgres',
              python_callable=insrt_postgres,
              op_kwargs={'postgres_conn_id':'conn-db-postgres-custom'}
       )

       insrt_postgres_with_hook