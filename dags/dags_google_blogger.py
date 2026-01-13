from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

import pendulum
import json


with DAG(
    dag_id="publish_blogger_post",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["blogger", "google", "publish"]
) as dag:

    @task
    def publish():
        # 1. Airflow Connection 로드
        conn = BaseHook.get_connection("google_blogger")
        extras = conn.extra_dejson or {}

        # 2. Credentials 생성
        creds = Credentials(
            token=None,
            refresh_token=extras["refresh_token"],
            token_uri=extras["token_uri"],
            client_id=extras["client_id"],
            client_secret=extras["client_secret"],
        )

        # 3. Blogger API Client
        service = build("blogger", "v3", credentials=creds)

        # 4. 게시글 정의
        post = {
            "title": "Airflow에서 자동 발행한 글",
            "content": """
                <p>이 글은 Airflow DAG에서 자동으로 발행되었습니다.</p>
                <p>Connection 기반 OAuth 인증 테스트 성공.</p>
            """
        }

        # 5. 게시
        response = service.posts().insert(
            blogId=extras["blog_id"],
            body=post,
            isDraft=False
        ).execute()

        # 6. 로그 출력
        print(f"Published post id: {response['id']}")
        print(f"Post URL: {response.get('url')}")

    publish()
