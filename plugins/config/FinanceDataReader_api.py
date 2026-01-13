import FinanceDataReader as fdr
import pandas as pd 
import pendulum

def get_prompt_for_chatgpt(yyyymmdd, market, cnt_thing):
    ticker_name_lst = []
    fluctuation_rate_lst = []
    return_prompt_lst = []
    
    date = pendulum.now('Asia/Seoul')
    before_year_info_date = date.subtract(years=1).replace(month=12, day=1).format("YYYY-MM-DD")
    yyyymmdd = '2026-01-13' #새벽에 돌리면 작동 안함
    column_lst = ['Open', 'High', 'Low', 'Close', 'Volume', 'Change', 'Code', '매출액',
       '영업이익', '영업이익(발표기준)', '세전계속사업이익', '당기순이익', '당기순이익(지배)', '당기순이익(비지배)',
       '자산총계', '부채총계', '자본총계', '자본총계(지배)', '자본총계(비지배)', '자본금', '영업활동현금흐름',
       '투자활동현금흐름', '재무활동현금흐름', 'CAPEX', 'FCF', '이자발생부채', '영업이익률', '순이익률',
       'ROE(%)', 'ROA(%)', '부채비율', '자본유보율', 'EPS(원)', 'PER(배)', 'BPS(원)',
       'PBR(배)', '현금DPS(원)', '현금배당수익률', '현금배당성향(%)', '발행주식수(보통주)']

    df = fdr.StockListing('KRX')
    cnt = 0
    rslt_tot_df = pd.DataFrame(columns=column_lst)

    for idx, ticker in enumerate(df['Code']) :
        try :        

            df_1 = fdr.DataReader(f'{ticker}', yyyymmdd)
            df_1['Code'] = f'{ticker}'

            df_2 = fdr.SnapDataReader(f'NAVER/FINSTATE/{ticker}')
            df_2 = df_2[df_2.index.strftime('%Y-%m-%d') == before_year_info_date] 
            df_2['Code'] = f'{ticker}'

            rslt_df = pd.merge(df_1, df_2, on='Code', how='inner')

            rslt_tot_df = pd.concat([rslt_tot_df, rslt_df], ignore_index = True)
            cnt = cnt + 1

        except : 
            pass
        if cnt == 5 : ##설정 안하면, 확인된 거 전부 돌림. (2시간 이상 소요)
            break
    
    tot_df = rslt_tot_df.sort_values(by=['Change'], ascending=False)
    tot_df.reset_index(inplace=True)

    # print(tot_df)
    for idx, row in tot_df.iterrows():

        ticker_name = row['Code'] #Ticker나 기업명 추가 필요
        fluc_rate = row['Change']
        open_value = row['Open']
        high_value = row['High']
        low_value = row['Low']
        end_value = row['Close']
        volume = row['Volume']

        bps = '' if pd.isna(row['BPS(원)']) else row['BPS(원)']
        per = '' if pd.isna(row['PER(배)']) else row['PER(배)']
        pbr = '' if pd.isna(row['PBR(배)']) else row['PBR(배)']
        eps = '' if pd.isna(row['EPS(원)']) else row['EPS(원)']
        div = '' if pd.isna(row['현금배당수익률']) else row['현금배당수익률']
        dps = '' if pd.isna(row['현금DPS(원)']) else row['현금DPS(원)']


        chatgpt_prompt = f'''
        오늘 KOSPI에서 {round(fluc_rate, 2)}%로 상승으로 마감한 종목코드 {ticker_name}에 대한 정보야.
        종목코드 {ticker_name}에 대한 회사 소개를 리포트로 만들어줘.
        종목코드를 기준으로 회사명을 찾아내줘
        그리고 아래 정보들도 포함해서 리포트로 만들어줘. 
        등락률: {round(fluc_rate, 2)}
        시가: {open_value}
        고가: {high_value}
        저가: {low_value}
        종가: {end_value}
        거래량: {volume}
        BPS: {bps}
        PER: {per}
        PBR: {pbr}
        EPS: {eps}
        DIV: {div}
        DPS: {dps}
        '''

        ticker_name_lst.append(ticker_name)
        return_prompt_lst.append(chatgpt_prompt)
        fluctuation_rate_lst.append(fluc_rate)

        if idx == cnt_thing-1:
            break
        print(len(ticker_name_lst), len(fluctuation_rate_lst), len(return_prompt_lst))
    return ticker_name_lst, fluctuation_rate_lst, return_prompt_lst