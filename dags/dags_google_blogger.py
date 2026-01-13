from airflow import DAG
# from airflow.decorators import task
# from airflow.hooks.base import BaseHook

# from google.oauth2.credentials import Credentials
# from googleapiclient.discovery import build


from operators.tistory_write_post_by_chatgpt_operator import TistoryWritePostByChatgptOperator

import pendulum
import json


with DAG(
    dag_id="publish_blogger_post",
    start_date=pendulum.datetime(2025, 1, 13, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["blogger", "google", "publish"]
) as dag:

    tistory_write_post_by_chatgpt = TistoryWritePostByChatgptOperator(
        task_id='tistory_write_post_by_chatgpt',
        post_cnt_per_market=3
    )
