import streamlit as st

from datetime import date, datetime, timezone, timedelta
import requests

import scipy.stats as ss
import pandas as pd
import json
import time



import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.io as io


# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title='KOR_CFPI',
    #page_icon=':earth_americas:', # This is an emoji shortcode. Could be a URL too.
)

url__krx_index = f'http://data-dbg.krx.co.kr/svc/apis/idx/krx_dd_trd'
krx_headers = {
    'AUTH_KEY' : st.secrets['krx_api_key'] # st.secrets['krx_api_key'],
    #'basDd' : "20200915"
              }

crisis_range = 2.5
bok_ecos_key = st.secrets['bok_ecos_key']
# st.secrets['bok_ecos_key']
# st.secrets['krx_api_key']


# 현재 시간 체크
time = datetime.now(timezone(timedelta(hours=9)))
data_date = (time - timedelta(1)) .date()
datetime_str = data_date.strftime("%Y%m%d")
요청_수 = 10000


# 데이터 다운

cur_krx은행_df = pd.read_excel("data/은행지수.xlsx").set_index('date')

time = datetime.now(timezone(timedelta(hours=9)))
# data_date = (time - timedelta(1)).date()
data_date = (time).date()
print(data_date)

if (data_date.isoweekday() <= 5) and (cur_krx은행_df.index.max().date() < data_date) :
    start_date = cur_krx은행_df.index.max().date() + timedelta(1) 
    end_date = data_date
    date_range = pd.date_range(start=start_date, end=end_date)

    krx_df_list=[]

    for date in date_range:
 
        response__krx_index = requests.get(url__krx_index, headers=krx_headers, params= { 'basDd' : date.strftime("%Y%m%d") } )
        if response__krx_index.status_code != 200:
            print( date, response__krx_index)
            break
        data_dict = json.loads(response__krx_index.text)
        data_list = data_dict["OutBlock_1"]
        cur_df = pd.DataFrame(data_list)
        krx_df_list.append(cur_df)

    krx_df = pd.concat(krx_df_list)
    add_krx은행_df = krx_df.query( 'IDX_NM == "KRX 은행" ')[['BAS_DD', 'CLSPRC_IDX',   'FLUC_RT']]
    add_krx은행_df.columns = ['date', '종가', '수정주가수익률']
    add_krx은행_df['date'] = pd.to_datetime (add_krx은행_df['date'] )
    add_krx은행_df.set_index('date', inplace = True)
    cur_krx은행_df = pd.concat( [cur_krx은행_df, add_krx은행_df ] )
    #cur_krx은행_df.to_excel("data/은행지수.xlsx")


##  CD 수익률 (91일)
url__cd_rtn = f"https://ecos.bok.or.kr/api/StatisticSearch/{bok_ecos_key}/xml/kr/1/{요청_수}/817Y002/D/19980101/{datetime_str}/010502000/?/?/?"
response__cd_rtn = requests.get(url__cd_rtn)

cd_수익률 = pd.read_xml( response__cd_rtn.text, xpath='.//row')
cd_수익률 = cd_수익률[['TIME', 'DATA_VALUE']]
cd_수익률.columns = ['date', 'CD 수익률']
cd_수익률['date']= pd.to_datetime(cd_수익률['date'], format= '%Y%m%d')
cd_수익률.set_index('date', inplace= True)

## 통안증권 수익률 (91일)
url__ms_rtn = f"https://ecos.bok.or.kr/api/StatisticSearch/{bok_ecos_key}/xml/kr/1/{요청_수}/817Y002/D/19980101/{datetime_str}/010400000/?/?/?"
response__ms_rtn = requests.get(url__ms_rtn)

통안증권_수익률 = pd.read_xml( response__ms_rtn.text, xpath='.//row')
통안증권_수익률 = 통안증권_수익률[['TIME', 'DATA_VALUE']]
통안증권_수익률.columns = ['date', '통안증권 수익률']
통안증권_수익률['date']= pd.to_datetime(통안증권_수익률['date'], format= '%Y%m%d')
통안증권_수익률.set_index('date', inplace= True)

## kospi
url__kospi_idx= f"https://ecos.bok.or.kr/api/StatisticSearch/{bok_ecos_key}/xml/kr/1/{요청_수}/802Y001/D/19970101/{datetime_str}/0001000/?/?/?"
response__kospi_idx = requests.get(url__kospi_idx)

코스피_지수 = pd.read_xml( response__kospi_idx.text, xpath='.//row')
코스피_지수 = 코스피_지수[['TIME', 'DATA_VALUE']]
코스피_지수.columns = ['date', 'KOSPI 지수']
코스피_지수['date']= pd.to_datetime(코스피_지수['date'], format= '%Y%m%d')
코스피_지수.set_index('date', inplace= True)

## 국채 3년
url__국채3년= f"https://ecos.bok.or.kr/api/StatisticSearch/{bok_ecos_key}/xml/kr/1/{요청_수}/817Y002/D/19980101/{datetime_str}/010200000/?/?/?"
response__국채3년 = requests.get(url__국채3년)
국채3년금리 = pd.read_xml( response__국채3년.text, xpath='.//row')

국채3년금리 = 국채3년금리[['TIME', 'DATA_VALUE']]
국채3년금리.columns = ['date', '국채 3년']
국채3년금리['date']= pd.to_datetime(국채3년금리['date'], format= '%Y%m%d')
국채3년금리.set_index('date', inplace= True)

## 통안증권 1년
url__통안증권1년= f"https://ecos.bok.or.kr/api/StatisticSearch/{bok_ecos_key}/xml/kr/1/{요청_수}/817Y002/D/19980101/{datetime_str}/010400001/?/?/?"
response__통안증권1년 = requests.get(url__통안증권1년)
통안증권1년금리 = pd.read_xml( response__통안증권1년.text, xpath='.//row')

통안증권1년금리 = 통안증권1년금리[['TIME', 'DATA_VALUE']]
통안증권1년금리.columns = ['date', '통안증권 1년']
통안증권1년금리['date']= pd.to_datetime(통안증권1년금리['date'], format= '%Y%m%d')
통안증권1년금리.set_index('date', inplace= True)

## 회사채 AA
url__회사채AA= f"https://ecos.bok.or.kr/api/StatisticSearch/{bok_ecos_key}/xml/kr/1/{요청_수}/817Y002/D/19980101/{datetime_str}/010300000/?/?/?"
response__회사채AA = requests.get(url__회사채AA)
회사채AA금리 = pd.read_xml( response__회사채AA.text, xpath='.//row')

회사채AA금리 = 회사채AA금리[['TIME', 'DATA_VALUE']]
회사채AA금리.columns = ['date', '회사채AA']
회사채AA금리['date']= pd.to_datetime(회사채AA금리['date'], format= '%Y%m%d')
회사채AA금리.set_index('date', inplace= True)

## 원달러 환율

url__usd_krw = f"https://ecos.bok.or.kr/api/StatisticSearch/{bok_ecos_key}/xml/kr/1/{요청_수}/731Y001/D/19980101/{datetime_str}/0000001/?/?/?"
response__usd_krw = requests.get(url__usd_krw)

달러환율 = pd.read_xml( response__usd_krw.text, xpath='.//row')
달러환율 = 달러환율[['TIME', 'DATA_VALUE']]
달러환율.columns = ['date', 'USD/KRW']
달러환율['date']= pd.to_datetime(달러환율['date'], format= '%Y%m%d')
달러환율.set_index('date', inplace= True)
달러환율 = 1/달러환율

달러환율 = 달러환율.rolling(window=21).std()
달러환율.columns = ['USD/KRW 환율 변동성']

# 지수 산출
# 은행 부분 압력지수 = KRX 은행지수 변동성 + CD스프레드

krx은행지수_변동성 = cur_krx은행_df['수정주가수익률'].rolling(window='180D').std()
CD_스프레드 = cd_수익률['CD 수익률'] - 통안증권_수익률['통안증권 수익률']

은행부문압력지수 = ss.zscore( pd.concat( 
        [ 
            CD_스프레드.loc['2007-01-01':],
            krx은행지수_변동성.loc['2007-01-01':]
        ], axis = 1 ).dropna().sum(axis = 1)
    )

# 채권 주식 부분 압력지수 
# = Kospi 지수 변동성 
# - kospi 지수 수익률 (12개월 최대하락폭) 
# - 기간프리미엄(국채 3년물 - 통안증권 1년물)
# + 회사채 스프레드 (회사채AA - 국채 3년)

코스피_지수['KOSPI 지수 수익률'] = 코스피_지수['KOSPI 지수'].pct_change() * 100
코스피_지수['KOSPI 지수 변동성'] = 코스피_지수['KOSPI 지수 수익률'].rolling(window='180D').std()
코스피_지수['KOSPI 지수 1년 최대값'] = 코스피_지수['KOSPI 지수'].rolling('365D').max() # 252 거래일
코스피_지수['KOSPI 지수 1년 DrawDown'] = (코스피_지수['KOSPI 지수'] - 코스피_지수['KOSPI 지수 1년 최대값'] ) * 100 /  코스피_지수['KOSPI 지수 1년 최대값']
코스피_지수['KOSPI 지수 1년 Max DrawDown'] = 코스피_지수['KOSPI 지수 1년 DrawDown'].rolling('365D').min()

기간_프리미엄 = 국채3년금리['국채 3년'] - 통안증권1년금리['통안증권 1년']
회사채_스프레드 = 회사채AA금리['회사채AA'] - 국채3년금리['국채 3년']

채권주식부문압력지수 = ss.zscore( pd.concat( [ 코스피_지수['KOSPI 지수 변동성'], 코스피_지수['KOSPI 지수 1년 Max DrawDown'] *-1, 기간_프리미엄*-1, 회사채_스프레드], axis = 1 ).dropna().sum(axis = 1).loc['2007-01-01':] )

# 외환부문압력지수  = USD/KRW 환율 변동성
외환부문압력지수 = ss.zscore( 달러환율['USD/KRW 환율 변동성'].loc['2007-01-01':] )

CFPI = pd.merge(pd.DataFrame( 은행부문압력지수, columns = ['은행부문압력지수'] ),
        pd.DataFrame( 채권주식부문압력지수, columns = ['채권주식부문압력지수'] ), 
        how = 'inner', 
        left_index = True, 
        right_index = True).merge( pd.DataFrame(외환부문압력지수),
                                how = 'inner', left_index = True, right_index = True ).sum(axis = 1)
print(은행부문압력지수.index.max)
print(채권주식부문압력지수.index.max)
print(외환부문압력지수.index.max)