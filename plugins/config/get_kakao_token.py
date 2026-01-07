import requests

client_id = '{restapikey}'
redirect_uri = 'https://example.com/oauth/2312312'
authorize_code = '{인가코드}'

token_url = 'https://kauth.kakao.com/oauth/token'
data = {
    'grant_type': 'authorization_code',
    'client_id': client_id,
    'redirect_uri': redirect_uri,
    'code': authorize_code
}

response = requests.post(token_url, data=data)
tokens = response.json()
print(tokens)