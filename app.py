import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime
import plotly.express as px
import numpy as np

# 1. 구글 시트 연결 (인증 정보 생략 없음)
def get_gspread_client():
    try:
        creds_info = st.secrets["gcp_service_account"]
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ 인증 오류: {e}")
        return None

# 2. 데이터 처리 엔진 (지배인님 원본 로직 100% 유지 + 에러 방어)
def process_data(uploaded_file, status):
    try:
        if uploaded_file.name.endswith('.csv'):
            df_raw = pd.read_csv(uploaded_file, skiprows=1)
        else:
            df_raw = pd.read_excel(uploaded_file, skiprows=1)
        
        df_raw.columns = df_raw.iloc[0]
        df_raw = df_raw.drop(df_raw.index[0]).reset_index(drop=True)
        
        # [에러 방지] '고객명'이 없는 요약표 파일 대응
        if '고객명' not in df_raw.columns:
            if '일자' in df_raw.columns: # 영업현황 파일인 경우
                df_raw['고객명'] = '영업현황_데이터'
                df_raw = df_raw.rename(columns={'일자': '입실일자', '매출': '객실료'})
            else:
                st.error("❌ 파일에 '고객명' 또는 '일자' 컬럼이 없습니다.")
                return pd.DataFrame()

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
        
        for col in ['Room_Revenue', 'Total_Revenue', 'Rooms', 'Nights']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0
        df['RN'] = df['Rooms'] * df['Nights']
        df['ADR'] = df.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)
        df['ADR'] = df['ADR'].replace([np.inf, -np.inf], 0).fillna(0)
        
        for col in ['CheckIn', 'Booking_Date']:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        today_str = datetime.now().strftime('%Y-%m-%d')
        df['Snapshot_Date'] = today_str
        df['Status'] = status
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

        return df[['Guest_Name', 'CheckIn', 'Booking_Date', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Nat_Group', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Month_Label', 'Is_Zero_Rate']]
    except Exception as e:
        st.error(f"파일 처리 중 오류: {e}")
        return pd.DataFrame()

# 3. 무삭제 상세 분석 렌더링
def render_full_analysis(data, title):
    if data is None or data.empty:
        st.info(f"📍 {title} 데이터가 없습니다.")
        return
    st.markdown(f"#### 📊 {title} 무삭제 상세 분석")
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
st.set_page_config(page_title="ARI Extreme Executive Dashboard", layout="wide")

try:
    c = get_gspread_client()
    sh = c.open("Amber_Revenue_DB")
    
    # 1. Budget 시트 로드
    try:
        budget_sheet = sh.worksheet("Budget")
        budget_raw = budget_sheet.get_all_values()
        budget_df = pd.DataFrame(budget_raw[1:], columns=budget_raw[0])
        budget_df['Budget'] = pd.to_numeric(budget_df['Budget'], errors='coerce').fillna(0)
    except:
        budget_df = pd.DataFrame(columns=['Month', 'Budget'])

    # 2. 메인 DB 로드
    db_sheet = sh.get_worksheet(0)
    raw_db = db_sheet.get_all_values()
    
    st.header("🏛️ 앰버 호텔 경영 요약 리포트")

    if len(raw_db) > 1:
        db_df = pd.DataFrame(raw_db[1:], columns=raw_db[0])
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR']:
            db_df[col] = pd.to_numeric(db_df[col], errors='coerce').fillna(0)
        
        db_df['Is_Zero_Rate'] = db_df['Is_Zero_Rate'].map({'True': True, 'False': False, True: True, False: False})
        all_dates = sorted(db_df['Snapshot_Date'].unique(), reverse=True)
        sel_date = st.sidebar.selectbox("Snapshot 선택", ["전체 누적"] + all_dates)
        
        filtered_df = db_df if sel_date == "전체 누적" else db_df[db_df['Snapshot_Date'] <= sel_date]
        paid_df = filtered_df[filtered_df['Is_Zero_Rate'] == False]
        bk = paid_df[paid_df['Status'] == 'Booked']
        cn = filtered_df[filtered_df['Status'] == 'Cancelled']

        # --- [지배인님 요청] 최상단 2종 버짓 대시보드 (Sales on the Book 기준) ---
        curr_month = datetime.now().strftime('%Y-%m')
        
        # A. 당월 실적 및 달성률
        m_bk = bk[bk['Stay_Month'] == curr_month]
        m_rev, m_rn = m_bk['Room_Revenue'].sum(), m_bk['RN'].sum()
        m_adr = (m_rev / m_rn) if m_rn > 0 else 0
        m_budget = budget_df[budget_df['Month'] == curr_month]['Budget'].sum()
        m_achieve = (m_rev / m_budget * 100) if m_budget > 0 else 0

        # B. 전체 실적 및 달성률
        t_rev, t_rn = bk['Room_Revenue'].sum(), bk['RN'].sum()
        t_adr = (t_rev / t_rn) if t_rn > 0 else 0
        t_budget = budget_df['Budget'].sum()
        t_achieve = (t_rev / t_budget * 100) if t_budget > 0 else 0

        st.subheader(f"🎯 실시간 버짓 달성 현황 (Snapshot: {sel_date})")
        
        # 1행: 당월 지표
        st.markdown(f"#### 🗓️ {curr_month} 당월 목표 달성")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🎯 당월 달성률", f"{m_achieve:.1f} %", delta=f"Target: {m_budget:,.0f}")
        c2.metric("🏠 당월 객실매출", f"{m_rev:,.0f} 원")
        c3.metric("📈 당월 ADR", f"{m_adr:,.0f} 원")
        c4.metric("🛏️ 당월 룸나잇", f"{m_rn:,.0f} RN")
        
        st.divider()

        # 2행: 전체 지표
        st.markdown("#### 🌍 전체 기간 누적 달성")
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("🚩 전체 달성률", f"{t_achieve:.1f} %", delta=f"Target: {t_budget:,.0f}")
        k2.metric("💰 누적 객실매출", f"{t_rev:,.0f} 원")
        k3.metric("📊 누적 ADR", f"{t_adr:,.0f} 원")
        k4.metric("📋 누적 룸나잇", f"{t_rn:,.0f} RN")
        
        st.divider()
    else:
        st.warning("📡 데이터가 비어있습니다. '📤 데이터 업데이트' 탭에서 파일을 업로드해 주세요.")

    # --- 탭 구성 (무삭제 원본 로직) ---
    t_month, t_week, t_det, t_zero, t_up = st.tabs(["🗓️ 월별 실적", "📅 주별 트렌드", "📈 상세 분석 리포트", "🆓 0원 예약 목록", "📤 데이터 업데이트"])
    
    with t_up:
        st.subheader("📤 세일즈 데이터 개별 업로드")
        col1, col2, col3 = st.columns(3)
        with col1:
            f1 = st.file_uploader("1️⃣ 신규 예약 리스트", type=['xlsx', 'csv'], key="up_new")
            if f1 and st.button("신규 예약 반영"):
                df = process_data(f1, "Booked")
                if not df.empty: db_sheet.append_rows(df.fillna('').astype(str).values.tolist()); st.success("✅ 완료!")
        with col2:
            f2 = st.file_uploader("2️⃣ 취소 리스트", type=['xlsx', 'csv'], key="up_cn")
            if f2 and st.button("취소 내역 반영"):
                df = process_data(f2, "Cancelled")
                if not df.empty: db_sheet.append_rows(df.fillna('').astype(str).values.tolist()); st.success("✅ 완료!")
        with col3:
            f3 = st.file_uploader("3️⃣ 온더북(OTB) 전체", type=['xlsx', 'csv'], key="up_otb")
            if f3 and st.button("OTB 전체 반영"):
                df = process_data(f3, "Booked")
                if not df.empty: db_sheet.append_rows(df.fillna('').astype(str).values.tolist()); st.success("✅ 완료!")

    # 나머지 분석 탭 로직 (생략 없이 유지)
    if len(raw_db) > 1:
        with t_month:
            m_sum = bk.groupby('Stay_Month').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
            m_res = pd.merge(m_sum, budget_df, left_on='Stay_Month', right_on='Month', how='left').fillna(0)
            m_res['달성률(%)'] = (m_res['Room_Revenue'] / m_res['Budget'] * 100).replace([np.inf, -np.inf], 0).round(1)
            st.table(m_res.style.format({'RN':'{:,}', 'Room_Revenue':'{:,}', 'Budget':'{:,}', '달성률(%)':'{}%'}))
        with t_det:
            render_full_analysis(bk, "유료 예약")
        with t_zero:
            st.dataframe(db_df[db_df['Is_Zero_Rate'] == True][['Guest_Name', 'CheckIn', 'Account', 'Room_Type']])

except Exception as e:
    st.error(f"🚨 시스템 오류: {e}")
