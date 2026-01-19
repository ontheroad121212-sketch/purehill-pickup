import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime
import plotly.express as px
import numpy as np

# 1. 구글 시트 연결 (인증 정보 무삭제)
def get_gspread_client():
    try:
        creds_info = st.secrets["gcp_service_account"]
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ 인증 오류: {e}")
        return None

# 2. 데이터 처리 엔진 (원본 로직 + OTB 합계 섹션 추출 + 소계 제거 통합)
def process_data(uploaded_file, status):
    try:
        # 파일 형식 판별 (영업현황 요약 vs 상세 리스트)
        is_otb = "Sales on the Book" in uploaded_file.name or "영업 현황" in uploaded_file.name
        
        if is_otb:
            if uploaded_file.name.endswith('.csv'):
                df_raw = pd.read_csv(uploaded_file, skiprows=3)
            else:
                df_raw = pd.read_excel(uploaded_file, skiprows=3)
            
            # 소계(Subtotal) 및 합계 행 즉시 제거
            df_raw = df_raw[df_raw['일자'].notna()]
            df_raw = df_raw[~df_raw['일자'].astype(str).str.contains('소계|Subtotal|합계|Total|합 계', na=False)]
            
            df = pd.DataFrame()
            df['Guest_Name'] = 'OTB_SUMMARY_DATA'
            df['CheckIn'] = pd.to_datetime(df_raw['일자'], errors='coerce')
            df['RN'] = pd.to_numeric(df_raw.iloc[:, 14], errors='coerce').fillna(0) # 합계-객실수
            df['Room_Revenue'] = pd.to_numeric(df_raw.iloc[:, 18], errors='coerce').fillna(0) # 합계-매출
            df['Total_Revenue'] = df['Room_Revenue']
            df['ADR'] = pd.to_numeric(df_raw.iloc[:, 16], errors='coerce').fillna(0) # 합계-객단가
            
            # 원본 호환성 기본값
            df['Booking_Date'] = df['CheckIn']
            df['Segment'] = 'OTB_Summary'
            df['Account'] = 'General'
            df['Room_Type'] = 'Standard'
            df['Nat_Orig'] = 'KOR'
        else:
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
            df = df_raw.rename(columns=col_map).copy()
            for col in ['Room_Revenue', 'Total_Revenue', 'Rooms', 'Nights']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            df['RN'] = df.get('Rooms', 0) * df.get('Nights', 1)
            df['ADR'] = df.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)

        # 공통 처리 (원본 기능 100% 복구)
        df['ADR'] = df['ADR'].replace([np.inf, -np.inf], 0).fillna(0)
        df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0
        df['Snapshot_Date'] = datetime.now().strftime('%Y-%m-%d')
        df['Status'] = status
        
        df['CheckIn_dt'] = pd.to_datetime(df['CheckIn'], errors='coerce')
        df['Stay_Month'] = df['CheckIn_dt'].dt.strftime('%Y-%m')
        df['Stay_YearWeek'] = df['CheckIn_dt'].dt.strftime('%Y-%U주')
        df['Day_of_Week'] = df['CheckIn_dt'].dt.day_name()
        
        # Lead Time & Labeling
        df['Booking_dt'] = pd.to_datetime(df.get('Booking_Date', df['CheckIn']), errors='coerce')
        df['Lead_Time'] = (df['CheckIn_dt'] - df['Booking_dt']).dt.days.fillna(0).astype(int)
        
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
        df['Month_Label'] = df['CheckIn_dt'].apply(get_month_label)

        df['CheckIn'] = df['CheckIn_dt'].dt.strftime('%Y-%m-%d')
        return df[['Guest_Name', 'CheckIn', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Nat_Group', 'Month_Label', 'Is_Zero_Rate']]
    except Exception as e:
        st.error(f"🚨 파일 처리 중 치명적 오류: {e}")
        return pd.DataFrame()

# 3. 상세 분석 렌더링
def render_full_analysis(data, title):
    if data is None or data.empty:
        st.info(f"📍 {title} 데이터가 없습니다.")
        return
    st.markdown(f"#### 📊 {title} 상세 분석 리포트")
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
st.set_page_config(page_title="ARI Executive Dashboard", layout="wide")

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

    st.header("🏛️ 앰버 호텔 경영 요약 리포트 (OTB 통합)")

    # 1. 사이드바 - 데이터 업데이트 (무삭제)
    st.sidebar.subheader("📤 데이터 통합 업로드")
    f_new = st.sidebar.file_uploader("1️⃣ 신규 예약 리스트", type=['xlsx', 'csv'], key="up_new")
    if f_new and st.sidebar.button("신규 예약 반영"):
        db_sheet.append_rows(process_data(f_new, "Booked").fillna('').astype(str).values.tolist())
        st.sidebar.success("완료!")

    f_cn = st.sidebar.file_uploader("2️⃣ 취소 리스트", type=['xlsx', 'csv'], key="up_cn")
    if f_cn and st.sidebar.button("취소 내역 반영"):
        db_sheet.append_rows(process_data(f_cn, "Cancelled").fillna('').astype(str).values.tolist())
        st.sidebar.success("완료!")

    f_otb = st.sidebar.file_uploader("3️⃣ 영업현황 (Sales on the Book)", type=['xlsx', 'csv'], key="up_otb")
    if f_otb and st.sidebar.button("영업현황 데이터 반영"):
        db_sheet.append_rows(process_data(f_otb, "Booked").fillna('').astype(str).values.tolist())
        st.sidebar.success("완료!")

    # 2. 대시보드 렌더링
    raw_db = db_sheet.get_all_values()
    if len(raw_db) > 1:
        df = pd.DataFrame(raw_db[1:], columns=raw_db[0])
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        if 'Is_Zero_Rate' not in df.columns:
            df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0
        else:
            df['Is_Zero_Rate'] = df['Is_Zero_Rate'].map({'True': True, 'False': False, True: True, False: False})

        all_dates = sorted(df['Snapshot_Date'].unique(), reverse=True)
        sel_date = st.sidebar.selectbox("Snapshot 선택", ["전체 누적"] + all_dates)
        
        f_df = df if sel_date == "전체 누적" else df[df['Snapshot_Date'] <= sel_date]
        paid_df = f_df[f_df['Is_Zero_Rate'] == False]
        
        # 상단 지표 계산 (영업현황 데이터 우선)
        otb_all = paid_df[(paid_df['Segment'] == 'OTB_Summary') & (paid_df['Status'] == 'Booked')]
        if otb_all.empty: otb_all = paid_df[paid_df['Status'] == 'Booked']
            
        curr_month = datetime.now().strftime('%Y-%m')
        m_otb = otb_all[otb_all['Stay_Month'] == curr_month]
        
        m_rev, m_rn = m_otb['Room_Revenue'].sum(), m_otb['RN'].sum()
        m_budget = budget_df[budget_df['Month'] == curr_month]['Budget'].sum()
        m_achieve = (m_rev / m_budget * 100) if m_budget > 0 else 0

        t_rev, t_rn = otb_all['Room_Revenue'].sum(), otb_all['RN'].sum()
        t_budget = budget_df['Budget'].sum()
        t_achieve = (t_rev / t_budget * 100) if t_budget > 0 else 0

        # --- [최상단] 2종 버짓 대시보드 ---
        st.subheader(f"🎯 실시간 버짓 달성 현황 ({sel_date})")
        colA, colB = st.columns(2)
        with colA:
            st.info(f"🗓️ {curr_month} 당월 목표")
            c1, c2, c3 = st.columns(3); c1.metric("달성률", f"{m_achieve:.1f} %", delta=f"T: {m_budget:,.0f}"); c2.metric("매출", f"{m_rev:,.0f} 원"); c3.metric("ADR", f"{(m_rev/m_rn if m_rn>0 else 0):,.0f} 원")
        with colB:
            st.info("🌍 전체 기간 누계")
            k1, k2, k3 = st.columns(3); k1.metric("전체 달성률", f"{t_achieve:.1f} %", delta=f"T: {t_budget:,.0f}"); k2.metric("누적 매출", f"{t_rev:,.0f} 원"); k3.metric("누적 ADR", f"{(t_rev/t_rn if t_rn>0 else 0):,.0f} 원")
        
        st.divider()

        # --- [원본 탭 기능 100% 복구] ---
        tab_month, tab_week, tab_det, tab_zero = st.tabs(["🗓️ 월별 분석", "📅 주별 분석", "📈 상세 분석 리포트", "🆓 0원 예약 목록"])
        
        with tab_month:
            m_sum = otb_all.groupby('Stay_Month').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
            m_res = pd.merge(m_sum, budget_df, left_on='Stay_Month', right_on='Month', how='left').fillna(0)
            m_res['달성률(%)'] = (m_res['Room_Revenue'] / m_res['Budget'] * 100).replace([np.inf, -np.inf], 0).round(1)
            st.table(m_res.style.format({'RN':'{:,}', 'Room_Revenue':'{:,}', 'Budget':'{:,}', '달성률(%)':'{}%'}))
        
        with tab_week:
            net_df = pd.concat([otb_all, f_df[f_df['Status'] == 'Cancelled'].assign(RN=lambda x: -x['RN'], Room_Revenue=lambda x: -x['Room_Revenue'])])
            w_sum = net_df.groupby('Stay_YearWeek').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
            st.plotly_chart(px.line(w_sum, x='Stay_YearWeek', y='Room_Revenue', markers=True, title="주별 순매출 추이"), use_container_width=True)

        with tab_det:
            res_bk = paid_df[(paid_df['Segment'] != 'OTB_Summary') & (paid_df['Status'] == 'Booked')]
            res_cn = f_df[(f_df['Segment'] != 'OTB_Summary') & (f_df['Status'] == 'Cancelled')]
            t1, t2 = st.tabs(["✅ 유료 예약 상세", "❌ 취소 리스트 상세"])
            with t1: render_full_analysis(res_bk, "유료 예약")
            with t2: render_full_analysis(res_cn, "취소 내역")

        with tab_zero:
            st.subheader("🆓 0원 예약 목록 (체험단/VIP 등)")
            st.dataframe(f_df[f_df['Is_Zero_Rate'] == True][['Guest_Name', 'CheckIn', 'Account', 'Room_Type']])
    else:
        st.warning("📡 사이드바에서 데이터를 먼저 업로드해 주세요.")

except Exception as e:
    st.error(f"🚨 시스템 오류: {e}")
