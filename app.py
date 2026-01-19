import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime
import plotly.express as px

# 1. 구글 시트 연결
def get_gspread_client():
    try:
        creds_info = st.secrets["gcp_service_account"]
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ 인증 오류: {e}")
        return None

# 2. 데이터 처리 엔진 (18개 컬럼 무삭제 로직 유지 및 에러 방어)
def process_data(uploaded_file, status):
    if uploaded_file.name.endswith('.csv'):
        df_raw = pd.read_csv(uploaded_file, skiprows=1)
    else:
        df_raw = pd.read_excel(uploaded_file, skiprows=1)
    
    df_raw.columns = df_raw.iloc[0]
    df_raw = df_raw.drop(df_raw.index[0]).reset_index(drop=True)
    df_raw = df_raw[df_raw['고객명'].notna()]
    df_raw = df_raw[~df_raw['고객명'].astype(str).str.contains('합계|Total|소계|합 계', na=False)]
    
    col_map = {
        '고객명': 'Guest_Name', '입실일자': 'CheckIn', '예약일자': 'Booking_Date',
        '객실수': 'Rooms', '박수': 'Nights', '객실료': 'Room_Revenue',
        '총금액': 'Total_Revenue', '시장': 'Segment', '거래처': 'Account',
        '객실타입': 'Room_Type', '국적': 'Nat_Orig'
    }
    
    existing_cols = [c for c in col_map.keys() if c in df_raw.columns]
    df = df_raw[existing_cols].rename(columns=col_map).copy()
    
    today_dt = datetime.now()
    today_str = today_dt.strftime('%Y-%m-%d')
    df['Snapshot_Date'] = today_str
    df['Status'] = status
    
    for col in ['Room_Revenue', 'Total_Revenue', 'Rooms', 'Nights']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    for col in ['CheckIn', 'Booking_Date']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
            
    df['RN'] = df['Rooms'] * df['Nights']
    # [방어] 0 나누기 및 Non-finite 에러 원천 차단
    df['ADR'] = df.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)
    df['Lead_Time'] = (df['CheckIn'] - df['Booking_Date']).dt.days.fillna(0).astype(int)
    df['Day_of_Week'] = df['CheckIn'].dt.day_name()
    
    df['Stay_YearWeek'] = df['CheckIn'].dt.strftime('%Y-%U주')
    df['Stay_Month'] = df['CheckIn'].dt.strftime('%Y-%m')
    
    def classify_nat(row):
        name, orig = str(row.get('Guest_Name', '')), str(row.get('Nat_Orig', '')).upper()
        if re.search('[가-힣]', name): return 'KOR'
        if any(x in orig for x in ['CHN', 'HKG', 'TWN', 'MAC']): return 'CHN'
        return 'OTH'
    df['Nat_Group'] = df.apply(classify_nat, axis=1)

    def get_month_label(row_dt):
        try:
            curr = datetime.now()
            offset = (row_dt.year - curr.year) * 12 + (row_dt.month - curr.month)
            if offset == 0: return "0.당월(M)"
            elif offset == 1: return "1.익월(M+1)"
            elif offset == 2: return "2.익익월(M+2)"
            elif offset >= 3: return "3.익익익월+(M+3~)"
            else: return "Past"
        except: return "Unknown"
    df['Month_Label'] = df['CheckIn'].apply(get_month_label)

    df['CheckIn'] = df['CheckIn'].dt.strftime('%Y-%m-%d')
    df['Booking_Date'] = df['Booking_Date'].dt.strftime('%Y-%m-%d')
    
    final_cols = ['Guest_Name', 'CheckIn', 'Booking_Date', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Nat_Group', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Month_Label']
    return df[final_cols], today_str

# 3. 상세 분석 렌더링 함수 (빈 데이터 에러 원천 차단)
def render_full_analysis(data, title):
    if data is None or data.empty:
        st.info(f"📍 현재 조회된 {title} 내역이 없습니다.")
        return
        
    st.markdown(f"#### 📊 {title} 무삭제 상세 분석 리포트")
    c1, c2 = st.columns(2)
    with c1:
        st.write("**🏢 거래처별 실적 (RN, 매출, ADR)**")
        acc = data.groupby('Account').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        # [수정] ADR 계산 후 결측치 처리 로직 강화
        acc['ADR'] = (acc['Room_Revenue'] / acc['RN']).replace([float('inf'), -float('inf')], 0).fillna(0).astype(int)
        st.table(acc.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))
    with c2:
        st.write("**🛏️ 객실 타입별 실적**")
        rt = data.groupby('Room_Type').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        rt['ADR'] = (rt['Room_Revenue'] / rt['RN']).replace([float('inf'), -float('inf')], 0).fillna(0).astype(int)
        st.table(rt.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))

    st.write("**📅 시점 매트릭스 (Segment x Month_Label)**")
    pivot = data.pivot_table(index='Segment', columns='Month_Label', values='RN', aggfunc='sum', fill_value=0)
    st.table(pivot)

    c3, c4 = st.columns(2)
    with c3:
        st.write("**🗓️ 투숙 월별 실적**")
        sm = data.groupby('Stay_Month').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
        sm['ADR'] = (sm['Room_Revenue'] / sm['RN']).replace([float('inf'), -float('inf')], 0).fillna(0).astype(int)
        st.table(sm.sort_values('Stay_Month').style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))
    with c4:
        st.write("**📆 요일별 입실 분석**")
        dow = data.groupby('Day_of_Week').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
        dow['ADR'] = (dow['Room_Revenue'] / dow['RN']).replace([float('inf'), -float('inf')], 0).fillna(0).astype(int)
        dow_order = {'Monday':0, 'Tuesday':1, 'Wednesday':2, 'Thursday':3, 'Friday':4, 'Saturday':5, 'Sunday':6}
        dow['sort'] = dow['Day_of_Week'].map(dow_order)
        st.table(dow.sort_values('sort').drop('sort', axis=1).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))

# 4. 트렌드 분석 모듈 (주별/월별 호출용)
def render_periodic_trend(data, group_col, label):
    if data is None or data.empty:
        st.warning(f"⚠️ {label} 분석을 위한 데이터가 부족합니다.")
        return
    st.markdown(f"#### 📈 {label} 매출 트렌드")
    summary = data.groupby(group_col).agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
    summary['ADR'] = (summary['Room_Revenue'] / summary['RN']).replace([float('inf'), -float('inf')], 0).fillna(0).astype(int)
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.plotly_chart(px.line(summary, x=group_col, y='Room_Revenue', markers=True), use_container_width=True)
    with col2:
        st.table(summary.sort_values(group_col).style.format({'RN':'{:,}', 'Room_Revenue':'{:,}', 'ADR':'{:,}'}))

# --- UI 메인 ---
st.set_page_config(page_title="ARI Extreme Pro Plus", layout="wide")
st.sidebar.header("🔍 분석 필터")

tab_up, tab_sum, tab_weekly, tab_monthly, tab_det = st.tabs([
    "📤 데이터 업로드", "📋 경영진 요약", "📅 주별 분석", "🗓️ 월별 분석", "📈 무삭제 상세 분석"
])

try:
    c = get_gspread_client()
    sh = c.open("Amber_Revenue_DB")
    raw = sh.get_worksheet(0).get_all_values()
    
    if len(raw) > 1:
        db_df = pd.DataFrame(raw[1:], columns=raw[0])
        # DB 컬럼 수치화 및 결측치 방어
        num_cols = ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Lead_Time']
        for col in num_cols:
            if col in db_df.columns:
                db_df[col] = pd.to_numeric(db_df[col], errors='coerce').fillna(0)
        
        all_dates = sorted(db_df['Snapshot_Date'].unique(), reverse=True)
        sel_date = st.sidebar.selectbox("Snapshot 선택", ["전체 누적"] + all_dates)
        
        # [근본 해결] 누적 필터링 및 데이터 분리
        filtered_df = db_df if sel_date == "전체 누적" else db_df[db_df['Snapshot_Date'] <= sel_date]
        bk = filtered_df[filtered_df['Status'] == 'Booked']
        cn = filtered_df[filtered_df['Status'] == 'Cancelled']
        net_df = pd.concat([bk, cn.assign(RN=-cn['RN'], Room_Revenue=-cn['Room_Revenue'])])

        with tab_sum:
            st.header(f"🏛️ 앰버 호텔 경영 보고서 ({sel_date})")
            if len(all_dates) >= 2:
                latest, prev = all_dates[0], all_dates[1]
                st.subheader(f"⚡ 실시간 픽업 요약 (Vs. {prev})")
                m1, m2, m3, m4 = st.columns(4)
                l_bk = db_df[(db_df['Snapshot_Date']==latest) & (db_df['Status']=='Booked')]
                p_bk = db_df[(db_df['Snapshot_Date']==prev) & (db_df['Status']=='Booked')]
                drn = l_bk['RN'].sum() - p_bk['RN'].sum()
                drev = l_bk['Room_Revenue'].sum() - p_bk['Room_Revenue'].sum()
                m1.metric("순증감 (RN)", f"{drn:,.0f}", delta=f"{drn:,.0f}")
                m2.metric("매출 증감", f"{drev:,.0f}", delta=f"{drev:,.0f}")
                m3.metric("최근 취소", f"{len(db_df[(db_df['Snapshot_Date']==latest) & (db_df['Status']=='Cancelled')])}건")
                m4.metric("픽업 ADR", f"{(drev/drn if drn!=0 else 0):,.0f}원")
            
            st.divider()
            c_left, c_right = st.columns([2, 1])
            with c_left:
                if not bk.empty:
                    st.plotly_chart(px.bar(bk.groupby('Stay_Month')['Room_Revenue'].sum().reset_index(), x='Stay_Month', y='Room_Revenue', title="투숙월별 예상 매출"), use_container_width=True)
            with c_right:
                if not bk.empty:
                    st.plotly_chart(px.pie(bk, values='Room_Revenue', names='Segment', title="채널 비중"), use_container_width=True)

        with tab_weekly:
            render_periodic_trend(net_df, 'Stay_YearWeek', '주별')
        with tab_monthly:
            render_periodic_trend(net_df, 'Stay_Month', '월별')
        with tab_det:
            st_net, st_bk, st_cn = st.tabs(["합산(Net)", "예약(Booked)", "취소(Cancelled)"])
            with st_net: render_full_analysis(net_df, "합산")
            with st_bk: render_full_analysis(bk, "예약")
            with st_cn: render_full_analysis(cn, "취소")

    with tab_up:
        m = st.radio("데이터 종류", ["신규 예약", "취소 내역"], horizontal=True)
        status = "Booked" if m == "신규 예약" else "Cancelled"
        f = st.file_uploader("파일 선택", type=['csv', 'xlsx'])
        if f and st.button("DB 저장하기"):
            df_p, _ = process_data(f, status)
            sh_ws = c.open("Amber_Revenue_DB").get_worksheet(0)
            sh_ws.append_rows(df_p.fillna('').astype(str).values.tolist())
            st.success("✅ 저장 완료!")

except Exception as e:
    st.error(f"🚨 시스템 오류 발생: {e}")
