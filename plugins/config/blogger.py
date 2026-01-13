from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

def publish(access_token, blog_name, title, content, tag_lst):
    # 1. Airflow Connection 로드
    # conn = BaseHook.get_connection("google_blogger")
    # extras = conn.extra_dejson or {}
    # print(extras)

    # 2. Credentials 생성
    # print('ChatGPT_1')
    creds = Credentials(
        token=None,
        refresh_token=access_token["refresh_token"],
        token_uri=access_token["token_uri"],
        client_id=access_token["client_id"],
        client_secret=access_token["client_secret"],
    )
    # print('ChatGPT_2')

    # 3. Blogger API Client
    service = build("blogger", "v3", credentials=creds)

    # 4. 게시글 정의
    # print('ChatGPT_3')
    post = {
        "title": f"{title}",
        "content": f"""
            <p><h2>{blog_name}</h2></p>
            <p>{content}</p>
            <p>{tag_lst}</p>
        """
    }
    # print('ChatGPT_4')
    # 5. 게시
    response = service.posts().insert(
        blogId=access_token["blog_id"],
        body=post,
        isDraft=False
    ).execute()

    # print('ChatGPT_5')
    # 6. 로그 출력
    print(f"Published post id: {response['id']}")
    print(f"Post URL: {response.get('url')}")