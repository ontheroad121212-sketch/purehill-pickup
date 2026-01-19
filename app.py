import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime
import plotly.express as px
import numpy as np

# 1. 구글 시트 연결 (인증 정보 전체 유지)
def get_gspread_client():
    try:
        creds_info = st.secrets["gcp_service_account"]
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ 인증 오류: {e}")
        return None

# 2. 데이터 처리 엔진 (원본 유지 + 영업현황 합계 추출 + 소계 제거)
def process_data(uploaded_file, status):
    try:
        # 영업현황(OTB) 파일은 4번째 줄(skiprows=3)부터 실제 데이터 헤더가 나옵니다.
        if "Sales on the Book" in uploaded_file.name or "영업 현황" in uploaded_file.name:
            if uploaded_file.name.endswith('.csv'):
                df_raw = pd.read_csv(uploaded_file, skiprows=3)
            else:
                df_raw = pd.read_excel(uploaded_file, skiprows=3)
            
            # [지시사항] 소계(Subtotal) 및 합계(Total) 행 완전 제거
            df_raw = df_raw[df_raw['일자'].notna()]
            df_raw = df_raw[~df_raw['일자'].astype(str).str.contains('소계|Subtotal|합계|Total|합 계', na=False)]
            
            # [핵심] 제일 오른쪽 합계 섹션 데이터 강제 매핑 (컬럼 인덱스 기준)
            # 합계 섹션: 객실수(14), 점유율(15), 객단가(16), RevPAR(17), 매출(18)
            df_processed = pd.DataFrame()
            df_processed['Guest_Name'] = 'OTB_SUMMARY_DATA'
            df_processed['CheckIn'] = pd.to_datetime(df_raw['일자'], errors='coerce')
            df_processed['RN'] = pd.to_numeric(df_raw.iloc[:, 14], errors='coerce').fillna(0) # 합계-객실수
            df_processed['Room_Revenue'] = pd.to_numeric(df_raw.iloc[:, 18], errors='coerce').fillna(0) # 합계-매출
            df_processed['Total_Revenue'] = df_processed['Room_Revenue']
            df_processed['ADR'] = pd.to_numeric(df_raw.iloc[:, 16], errors='coerce').fillna(0) # 합계-객단가
            
            # 원본 로직 유지를 위한 필수 컬럼 기본값 채우기
            df_processed['Booking_Date'] = df_processed['CheckIn']
            df_processed['Segment'] = 'OTB_Summary'  # KPI 구분용 라벨
            df_processed['Account'] = 'General'
            df_processed['Room_Type'] = 'Standard'
            df_processed['Nat_Orig'] = 'KOR'
            df_processed = df_processed.dropna(subset=['CheckIn'])
        else:
            # 기존 상세 예약 리스트/취소 리스트 처리 로직 (생략 없이 유지)
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
            df_processed = df_raw.rename(columns=col_map).copy()
            for col in ['Room_Revenue', 'Total_Revenue', 'Rooms', 'Nights']:
                if col in df_processed.columns:
                    df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0)
            df_processed['RN'] = df_processed.get('Rooms', 0) * df_processed.get('Nights', 1)
            df_processed['ADR'] = df_processed.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)

        # 공통 데이터 정제 (원본 18개 컬럼 기준)
        df_processed['ADR'] = df_processed['ADR'].replace([np.inf, -np.inf], 0).fillna(0)
        df_processed['Is_Zero_Rate'] = df_processed['Total_Revenue'] <= 0
        df_processed['Snapshot_Date'] = datetime.now().strftime('%Y-%m-%d')
        df_processed['Status'] = status
        df_processed['Stay_Month'] = pd.to_datetime(df_processed['CheckIn']).dt.strftime('%Y-%m')
        df_processed['Stay_YearWeek'] = pd.to_datetime(df_processed['CheckIn']).dt.strftime('%Y-%U주')
        
        # Lead_Time 및 Day_of_Week 추가
        df_processed['CheckIn_dt'] = pd.to_datetime(df_processed['CheckIn'])
        df_processed['Booking_dt'] = pd.to_datetime(df_processed.get('Booking_Date', df_processed['CheckIn']))
        df_processed['Lead_Time'] = (df_processed['CheckIn_dt'] - df_processed['Booking_dt']).dt.days.fillna(0).astype(int)
        df_processed['Day_of_Week'] = df_processed['CheckIn_dt'].dt.day_name()
        
        df_processed['CheckIn'] = df_processed['CheckIn_dt'].dt.strftime('%Y-%m-%d')
        if 'Booking_Date' in df_processed.columns:
            df_processed['Booking_Date'] = df_processed['Booking_dt'].dt.strftime('%Y-%m-%d')

        return df_processed[['Guest_Name', 'CheckIn', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Is_Zero_Rate']]
    except Exception as e:
        st.error(f"🚨 파일 처리 중 치명적 오류: {e}")
        return pd.DataFrame()

# 3. 상세 분석 렌더링
def render_full_analysis(data, title):
    if data is None or data.empty:
        st.info(f"📍 {title} 데이터가 없습니다.")
        return
    st.markdown(f"#### 📊 {title} 무삭제 상세 분석 리포트")
    c1, c2 = st.columns(2)
    with c1:
        st.write("**🏢 거래처별 실적 (RN, 매출, ADR)**")
        acc = data.groupby('Account').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        acc['ADR'] = (acc['Room_Revenue'] / acc['RN']).replace([np.inf, -np.inf], 0).fillna(0).astype(int)
        st.table(acc.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))
    with c2:
        st.write("**🛏️ 객실 타입별 실적**")
        rt = data.groupby('Room_Type').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        rt['ADR'] = (rt['Room_Revenue'] / rt['RN']).replace([np.inf, -np.inf], 0).fillna(0).astype(int)
        st.table(rt.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))

# --- UI 메인 ---
st.set_page_config(page_title="ARI Extreme Pro Dashboard", layout="wide")

try:
    c = get_gspread_client()
    sh = c.open("Amber_Revenue_DB")
    db_sheet = sh.get_worksheet(0)
    
    # Budget 로드
    try:
        budget_raw = sh.worksheet("Budget").get_all_values()
        budget_df = pd.DataFrame(budget_raw[1:], columns=budget_raw[0])
        budget_df['Budget'] = pd.to_numeric(budget_df['Budget'], errors='coerce').fillna(0)
    except:
        budget_df = pd.DataFrame(columns=['Month', 'Budget'])

    st.header("🏛️ 앰버 호텔 실시간 경영 리포트")

    # 1. 사이드바 - 파일 3종 개별 업로드 섹션
    st.sidebar.subheader("📤 데이터 업로드 센터")
    f_new = st.sidebar.file_uploader("1️⃣ 신규 예약 리스트", type=['xlsx', 'csv'], key="up_new")
    if f_new and st.sidebar.button("신규 예약 반영"):
        db_sheet.append_rows(process_data(f_new, "Booked").fillna('').astype(str).values.tolist())
        st.sidebar.success("✅ 반영 완료!")

    f_cn = st.sidebar.file_uploader("2️⃣ 취소 리스트", type=['xlsx', 'csv'], key="up_cn")
    if f_cn and st.sidebar.button("취소 내역 반영"):
        db_sheet.append_rows(process_data(f_cn, "Cancelled").fillna('').astype(str).values.tolist())
        st.sidebar.success("✅ 반영 완료!")

    f_otb = st.sidebar.file_uploader("3️⃣ 세일즈온더북 (영업현황)", type=['xlsx', 'csv'], key="up_otb")
    if f_otb and st.sidebar.button("영업현황 데이터 반영"):
        db_sheet.append_rows(process_data(f_otb, "Booked").fillna('').astype(str).values.tolist())
        st.sidebar.success("✅ 반영 완료!")

    # 2. 대시보드 렌더링 (영업현황 데이터 우선 추출)
    raw_db = db_sheet.get_all_values()
    if len(raw_db) > 1:
        df = pd.DataFrame(raw_db[1:], columns=raw_db[0])
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        df['Is_Zero_Rate'] = df['Is_Zero_Rate'].map({'True': True, 'False': False, True: True, False: False})
        
        # [핵심] 모든 매출 지표는 Segment == 'OTB_Summary' (영업현황)에서만 가져옴
        otb_all = df[(df['Segment'] == 'OTB_Summary') & (df['Status'] == 'Booked')]
        
        curr_month = datetime.now().strftime('%Y-%m')
        
        # A. 당월(Month) 실적 및 달성률
        m_otb = otb_all[otb_all['Stay_Month'] == curr_month]
        m_rev, m_rn = m_otb['Room_Revenue'].sum(), m_otb['RN'].sum()
        m_budget = budget_df[budget_df['Month'] == curr_month]['Budget'].sum()
        m_achieve = (m_rev / m_budget * 100) if m_budget > 0 else 0

        # B. 전체(Total) 누적 실적 및 달성률
        t_rev, t_rn = otb_all['Room_Revenue'].sum(), otb_all['RN'].sum()
        t_budget = budget_df['Budget'].sum()
        t_achieve = (t_rev / t_budget * 100) if t_budget > 0 else 0

        st.subheader("🎯 실시간 버짓 달성 현황 (기준: 영업현황 데이터)")
        colA, colB = st.columns(2)
        with colA:
            st.info(f"🗓️ {curr_month} 당월 버짓 현황")
            c1, c2, c3 = st.columns(3)
            c1.metric("당월 달성률", f"{m_achieve:.1f} %", delta=f"Target: {m_budget:,.0f}")
            c2.metric("당월 객실매출", f"{m_rev:,.0f} 원")
            c3.metric("당월 ADR", f"{(m_rev/m_rn if m_rn > 0 else 0):,.0f} 원")
        with colB:
            st.info("🌍 전체 기간 누적 버짓 현황")
            k1, k2, k3 = st.columns(3)
            k1.metric("전체 달성률", f"{t_achieve:.1f} %", delta=f"Target: {t_budget:,.0f}")
            k2.metric("누적 객실매출", f"{t_rev:,.0f} 원")
            k3.metric("누적 ADR", f"{(t_rev/t_rn if t_rn > 0 else 0):,.0f} 원")
        
        st.divider()

        # 하단 상세 분석 (기존 상세 리스트 데이터 활용)
        tab1, tab2, tab3 = st.tabs(["🗓️ 월별 달성 상세", "📈 상세 분석 리포트", "🆓 0원 예약 목록"])
        with tab1:
            m_sum = otb_all.groupby('Stay_Month').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
            m_res = pd.merge(m_sum, budget_df, left_on='Stay_Month', right_on='Month', how='left').fillna(0)
            m_res['달성률(%)'] = (m_res['Room_Revenue'] / m_res['Budget'] * 100).replace([np.inf, -np.inf], 0).round(1)
            st.table(m_res.style.format({'RN':'{:,}', 'Room_Revenue':'{:,}', 'Budget':'{:,}', '달성률(%)':'{}%'}))
        with tab2:
            detail_bk = df[(df['Segment'] != 'OTB_Summary') & (df['Status'] == 'Booked')]
            render_full_analysis(detail_bk, "예약 리스트 기반 상세")
        with tab3:
            st.dataframe(df[df['Is_Zero_Rate'] == True][['Guest_Name', 'CheckIn', 'Account', 'Room_Type']])
    else:
        st.warning("📡 사이드바에서 영업현황(OTB) 또는 예약 리스트 파일을 업로드해 주세요.")

except Exception as e:
    st.error(f"🚨 시스템 오류: {e}")
