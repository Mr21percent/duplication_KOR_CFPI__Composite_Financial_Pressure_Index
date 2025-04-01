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

# cache를 통한 data 1회 연산
@st.cache_resource ( ttl = 28800)
def calc_cfpi():
    
    # 현재 시간 체크
    time = datetime.now(timezone(timedelta(hours=9)))
    data_date = (time - timedelta(1)) .date()
    datetime_str = data_date.strftime("%Y%m%d")
    요청_수 = 10000


    # 데이터 다운

    cur_krx은행_df = pd.read_excel("data/은행지수.xlsx").set_index('date')
    
    time = datetime.now(timezone(timedelta(hours=9)))
    data_date = (time - timedelta(1)).date()
        
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
    return CFPI, 은행부문압력지수.loc[ CFPI.index ], 채권주식부문압력지수.loc[ CFPI.index ], 외환부문압력지수.loc[ CFPI.index ]

def bgLevels(fig, df, variable, level, mode, fillcolor, layer):
    """
    Set a specified color as background for given
    levels of a specified variable using a shape.
    
    Keyword arguments:
    ==================
    fig -- plotly figure
    variable -- column name in a pandas dataframe
    level -- int or float
    mode -- set threshold above or below
    fillcolor -- any color type that plotly can handle
    layer -- position of shape in plotly fiugre, like "below"
    
    """
    
    if mode == 'above':
        m = df[variable].gt(level)
    
    if mode == 'below':
        m = df[variable].lt(level)
        
    df1 = df[m].groupby((~m).cumsum())['date'].agg(['first','last'])

    for index, row in df1.iterrows():
        #print(row['first'], row['last'])
        fig.add_shape(type="rect",
                        xref="x",
                        yref="paper",
                        x0=row['first'],
                        y0=0,
                        x1=row['last'],
                        y1=1,
                        line=dict(color="rgba(0,0,0,0)",width=3,),
                        fillcolor=fillcolor,
                        layer=layer) 
    return(fig)



#st.dataframe( CFPI )
#st.pyplot( CFPI.plot() )


io.renderers.default='browser' #이거 해야 ploty 실시간 확인 가능 fig.show()가 인터넷 창에

st.markdown( '''
            이 페이지는 BOK 이슈노트 [제2024-11호] [데이터 기반 금융·외환 조기경보모형](https://www.bok.or.kr/portal/bbs/P0002353/view.do?nttId=10083742&searchCnd=1&searchKwd=&depth2=201156&depth3=200433&depth=200433&pageUnit=10&pageIndex=1&programType=newsData&menuNo=200433&oldMenuNo=200433)에서 소개된 **복합 금융 압력 지수 (CFPI)** 를 산출하여 나타내는 페이지 입니다. 
            일부 변수들의 수정을 거쳐 일별로 산출이 가능하도록 수정하였습니다. 오류나 개선 사항은 woneunglee@hanyang.ac.kr로 연락주시면 수정하겠습니다.
            ''')

st.text(datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d"))

st.title(" 복합 금융 압력 지수 (CFPI)")

st.markdown( '''

조기경보 대상 이벤트가 실제 위기로 이어진 것인지 아니면 시장불안에 불과한지에 대한 컨센서스가 형성되기 어렵고, 그 위기가 언제 시작되었는지 특정하기도 어렵다.

이에 본 연구는 [Cardarelli et al. (2009)](https://www.imf.org/external/pubs/ft/wp/2009/wp09100.pdf)을 참고하여 은행, 채권 주식, 외환 등 부문별압력지수로 구성된 복합금융압력지수(CFPI, Composit Financial Pressure Index)를 정의하고 조기경보대상 이벤트를 식별하였다.

### 복합금융압력지수(CFPI)
- 은행부문압력지수
  =KRX 은행지수 변동성 + CD스프레드 (CD수익률 - 통안증권 수익률)
  
- 채권 주식부문 압력지수
  =Kospi 지수 변동성
  -kospi 지수 수익률 (12개월 최대하락폭)
  -기간프리미엄(국채 3년물 - 통안증권 1년물)
  +회사채 스프레드 (회사채AA - 국채 3년)
  
- 외환부문압력지수
  =USD/KRW 환율 변동성
            ''')


with st.spinner('데이터를 불러오고 있는 중입니다. 10분 이상 소요될 수 도 있습니다...'):
    CFPI, 은행부문압력지수, 채권주식부문압력지수, 외환부문압력지수  = calc_cfpi()


check_crisis= st.checkbox(' 위기 구간 표기 (±2.5 초과)')
is_indiv_bar = st.toggle("개별 부문 bar 그래프 표기")

full_df = pd.concat([
    CFPI,
    외환부문압력지수,
    채권주식부문압력지수,
    은행부문압력지수,
    ], axis = 1 )
full_df.columns = ['CFPI', '외환부문압력지수', '채권주식부분압력지수', '은행부문압력지수']
#st.dataframe(  full_df,)


# 차트
main_fig = make_subplots(specs=[[{"secondary_y": True}]])

main_fig.add_traces(
    [
     go.Scatter(x = full_df.index, 
                y = full_df['CFPI'],
                name = "CFPI"
                ),
     ])
if is_indiv_bar:
    main_fig.add_traces(
        [
        go.Bar(x=full_df.index, y=full_df['은행부문압력지수'], name='은행 부문'),
        go.Bar(x=full_df.index, y=full_df['채권주식부분압력지수'], name='채권 주식 부문'),
        go.Bar(x=full_df.index, y=full_df['외환부문압력지수'], name='외환 부문'),
        ]
)
main_fig.update_layout(barmode='stack', title_text="CFPI 지수")

if check_crisis:
    is_crisis_df = CFPI.to_frame().reset_index()
    is_crisis_df['is_crisis'] = (is_crisis_df[0].abs() > crisis_range).astype(float)

    main_fig = bgLevels(main_fig, 
                is_crisis_df, 
                'is_crisis', 
                level = 0.5, 
                mode = 'above', 
                fillcolor = 'rgba(200, 0, 0, 0.7)', 
                layer ='below')

st.plotly_chart(main_fig, use_container_width=True)


st.subheader( " 변수 설명 ")
st.html('''
<table style="border-collapse:collapse;border-color:#9ABAD9;border-spacing:0" class="tg"><thead>
<tr><th style="background-color:#409cff;border-color:#000000;border-style:solid;border-width:1px;color:#fff;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal"></th><th style="background-color:#409cff;border-color:#333333;border-style:solid;border-width:1px;color:#fff;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">변수명</th><th style="background-color:#409cff;border-color:#333333;border-style:solid;border-width:1px;color:#fff;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">산출법</th>
<th style="background-color:#409cff;border-color:#333333;border-style:solid;border-width:1px;color:#fff;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">데이터 출처</th></tr>
</thead>
<tbody>
<tr><td style="background-color:#D2E4FC;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal" rowspan="2">은행 부문 압력지수</td><td style="background-color:#D2E4FC;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">KRX 은행지수 변동성</td><td style="background-color:#D2E4FC;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal"></td>
<td style="background-color:#D2E4FC;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal"><a href="http://openapi.krx.co.kr/contents/OPP/MAIN/main/index.cmd" target="_blank" rel="noopener noreferrer">한국거래소 정보데이터시스템 OPEN API</a></td></tr>
<tr><td style="background-color:#EBF5FF;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">CD 스프레드</td><td style="background-color:#EBF5FF;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">CD 91일물 수익률 - 통안증권 91일물 수익률</td><td style="background-color:#EBF5FF;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal" rowspan="6"><a href="https://ecos.bok.or.kr/#/" target="_blank" rel="noopener noreferrer">ECOS - 한국은행 경제통계시스템</a></td></tr>
<tr><td style="background-color:#D2E4FC;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal" rowspan="4">채권 주식 부문 압력지수</td><td style="background-color:#D2E4FC;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">코스피 지수 변동성</td><td style="background-color:#D2E4FC;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">코스피 지수 수익률의 180 거래일 표준편차</td></tr>
<tr><td style="background-color:#EBF5FF;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">코스피 지수 수익률<br>(12개월 최대 하락폭)</td><td style="background-color:#EBF5FF;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">코스피 지수 1Yr&nbsp;&nbsp;MDD (Max DrawDown)</td></tr>
<tr><td style="background-color:#D2E4FC;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">기간프리미엄</td><td style="background-color:#D2E4FC;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">국채 3년물 수익률 - 통안증권 1년물 수익률</td></tr>
<tr><td style="background-color:#EBF5FF;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">회사채 스프레드</td><td style="background-color:#EBF5FF;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">회사채 AA 수익률 - 국채 3년물 수익률</td></tr>
<tr><td style="background-color:#D2E4FC;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">외환 부문 압력지수</td><td style="background-color:#D2E4FC;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">USD/KRW 환율 변동성</td><td style="background-color:#D2E4FC;border-color:#333333;border-style:solid;border-width:1px;color:#444;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:center;vertical-align:top;word-break:normal">USD/KRW의 21 거래일 표준편차</td></tr></tbody></table>             
        ''')



# 하위 3단 차트

은행_fig = make_subplots()
은행_fig.add_traces(
    [
        go.Scatter(x = full_df.index, 
                    y = full_df['은행부문압력지수'],
                    name = "은행 부문 압력 지수"
                    ),
    ]
)
은행_fig.update_layout(title_text="은행 부문 압력 지수")
st.plotly_chart( 은행_fig )

채권_fig = make_subplots()
채권_fig.add_traces(
    [
        go.Scatter(x = full_df.index, 
                    y = full_df['채권주식부분압력지수'],
                    name = "채권 주식 부문 압력 지수"
                    ),
    ]
)
채권_fig.update_layout(title_text="채권 주식 부문 압력 지수")
st.plotly_chart( 채권_fig )

외환_fig = make_subplots()
외환_fig.add_traces(
    [
        go.Scatter(x = full_df.index, 
                    y = full_df['외환부문압력지수'],
                    name = "외환 부문 압력 지수"
                    ),
    ]
)
외환_fig.update_layout(title_text="외환 부문 압력 지수")
st.plotly_chart( 외환_fig )

st.subheader("데이터 표 ")
st.text('테이블 우측 상단에 다운로드 버튼이 있습니다.')
st.dataframe(full_df.loc['2008-01-01':])