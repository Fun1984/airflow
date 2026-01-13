import FinanceDataReader as fdr
import pandas as pd 
import pendulum

def get_prompt_for_chatgpt(yyyymmdd, market, cnt_thing):
    ticker_name_lst = []
    fluctuation_rate_lst = []
    return_prompt_lst = []
    # ohlcv_df = stock.get_market_ohlcv(date=yyyymmdd, market=market) # (market: KOSPI/KOSDAQ/KONEX/ALL)
    # fund_df = stock.get_market_fundamental(yyyymmdd, market=market)

    # tot_df = ohlcv_df.join(fund_df, how='inner')
    # tot_df = tot_df.sort_values(by=['등락률'], ascending=False)
    # tot_df.reset_index(inplace=True)
    date = pendulum.now('Asia/Seoul')
    before_year_info_date = date.substrack(years=1).replace(month=12, day=1).format("YYYY-MM-DD")
    print("date", date)
    print("before___", before_year_info_date)

    column_lst = ['Open', 'High', 'Low', 'Close', 'Volume', 'Change', 'Code', '매출액',
       '영업이익', '영업이익(발표기준)', '세전계속사업이익', '당기순이익', '당기순이익(지배)', '당기순이익(비지배)',
       '자산총계', '부채총계', '자본총계', '자본총계(지배)', '자본총계(비지배)', '자본금', '영업활동현금흐름',
       '투자활동현금흐름', '재무활동현금흐름', 'CAPEX', 'FCF', '이자발생부채', '영업이익률', '순이익률',
       'ROE(%)', 'ROA(%)', '부채비율', '자본유보율', 'EPS(원)', 'PER(배)', 'BPS(원)',
       'PBR(배)', '현금DPS(원)', '현금배당수익률', '현금배당성향(%)', '발행주식수(보통주)']

    df = fdr.StockListing('KRX')
    cnt = 0
    rslt_tot_df = pd.DataFrame(columns=column_lst)
    print("Good_1")

    for idx, ticker in enumerate(df['Code']) :
        try :        
            print(f"Good_2_{cnt}")
            df_1 = fdr.DataReader(f'{ticker}', yyyymmdd)
            df_1['Code'] = f'{ticker}'
            df_2 = fdr.SnapDataReader(f'NAVER/FINSTATE/{ticker}')
            df_2 = df_2[df_2.index == before_year_info_date] 
            df_2['Code'] = f'{ticker}'
            rslt_df = df_1.merge(df_2, how='inner')
            rslt_tot_df = pd.concat([rslt_tot_df, rslt_df], ignore_index = True)
            cnt += 1
            print(f"Good_2_{cnt}")
        except : 
            pass
        if cnt == 10 :
            break
    
    print(f"Good_3")
    tot_df = rslt_tot_df.sort_values(by=['Change'], ascending=False)
    print(f"Good_4")
    for idx, row in tot_df.iterrows():
        ticker_name = tot_df(row['Code'])
        fluc_rate = row['Change']
        open_value = row['Open']
        high_value = row['High']
        low_value = row['Low']
        end_value = row['Close']
        volume = row['Volume']
        bps = row['BPS(원)']
        per = row['PER(배)']
        pbr = row['PBR(배)']
        eps = row['EPS(원)']
        div = row['현금배당수익률']
        dps = row['현금DPS(원)']
        chatgpt_prompt = f'''
        오늘 KOSPI에서 {round(fluc_rate, 2)}%로 상승으로 마감한 {ticker_name}에 대한 정보야.
        {ticker_name}에 대한 회사 소개를 리포트로 만들어줘.
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

        if idx >= cnt_thing-1:
            break
    print(f"Good_5")
    return ticker_name_lst, fluctuation_rate_lst, return_prompt_lst