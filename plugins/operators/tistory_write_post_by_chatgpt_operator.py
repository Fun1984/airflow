from config.chatgpt import get_chatgpt_response
from config.FinanceDataReader_api import get_prompt_for_chatgpt
from config.blogger import publish
import pendulum
from random import randrange

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.sdk import Variable

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow.models.baseoperator import BaseOperator
#from airflow.models import Variable


class TistoryWritePostByChatgptOperator(BaseOperator):

    def __init__(self, post_cnt_per_market: int, **kwargs):
        super().__init__(**kwargs)
        self.post_cnt_per_market = post_cnt_per_market
    
        # tistory_access_token = 
        conn = BaseHook.get_connection("google_blogger")
        self.blogger_access_token = conn.extra_dejson or {}
        # print(extras)
    
    def execute(self, context):
        chatgpt_api_key = Variable.get('chatgpt_api_key')

        # print('chatgpt_api_key :', chatgpt_api_key)
        # print('google_blogger :', self.blogger_access_token)
        print('TTTTTT_1')
        now =  pendulum.now('Asia/Seoul')
        now_yyyymmmdd = now.strftime('%Y-%m-%d')
        yyyy = now.year
        mm = now.month
        dd = now.day
        hh = now.hour
        print('TTTTTT_2')
        # kospi_ticker_name_lst, kospi_fluctuation_rate_lst, prompt_of_kospi_top_n_lst = get_prompt_for_chatgpt(now_yyyymmmdd, market='KRX', cnt=self.post_cnt_per_market)
        # kosdaq_ticker_name_lst, kosdaq_fluctuation_rate_lst, prompt_of_kosdaq_top_n_lst = get_prompt_for_chatgpt(now_yyyymmmdd, market='KOSDAQ', cnt=self.post_cnt_per_market)
        krx_ticker_name_lst, krx_fluctuation_rate_lst, prompt_of_krx_top_n_lst = get_prompt_for_chatgpt(now_yyyymmmdd, market='KRX', cnt_thing=self.post_cnt_per_market)
        print('TTTTTT_3')
        tot_ticker_name_lst = krx_ticker_name_lst #+ kosdaq_ticker_name_lst
        tot_fluctuation_rate_lst = krx_fluctuation_rate_lst #+ kosdaq_fluctuation_rate_lst
        tot_prompt = prompt_of_krx_top_n_lst #+ prompt_of_kosdaq_top_n_lst
        print('TTTTTT_4', tot_prompt)

        market = 'KOSPI'
        for idx, prompt in enumerate(tot_prompt):
            print('TTT_1')
            temperature = randrange(10,100)/100     # 0.1 ~ 1 사이 
            ticker_name = tot_ticker_name_lst[idx]
            print(f'ticker: {ticker_name}, temperature:{temperature}')      # temperature 확인용 로깅

            fluctuation_rate = tot_fluctuation_rate_lst[idx]
            fluctuation_rate = round(fluctuation_rate, 1)
            chatgpt_resp = get_chatgpt_response(api_key=chatgpt_api_key, 
                                                prompt=prompt,
                                                temperature=temperature)
            chatgpt_resp = chatgpt_resp.replace('\n','<br/>')

            if idx >= self.post_cnt_per_market:
                market = 'KOSDAQ'
            print('TTT_2')
            publish(access_token=self.blogger_access_token,
                             blog_name='Please_Jebal_DaeRa',
                             title=f'{yyyy}/{mm}/{dd} {hh}시 {market} 급등 {fluctuation_rate}% {ticker_name} 주목!',
                             content=chatgpt_resp,
                             tag_lst=[f'{market}급등','급등주',ticker_name])
            print('TTT_3')
