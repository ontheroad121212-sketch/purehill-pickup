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

# 2. 데이터 처리 엔진 (기존 상세 리스트 + 영업현황 요약본 모두 대응)
def process_data(uploaded_file, status):
    try:
        # 파일 이름이나 구조로 영업현황(OTB Summary)인지 상세 리스트인지 판별
        is_otb_summary = "Sales on the Book" in uploaded_file.name or "영업 현황" in uploaded_file.name
        
        if is_otb_summary:
            # [영업현황] 4번째 줄부터 데이터 시작, 소계 제거, 오른쪽 합계 섹션 추출
            if uploaded_file.name.endswith('.csv'):
                df_raw = pd.read_csv(uploaded_file, skiprows=3)
            else:
                df_raw = pd.read_excel(uploaded_file, skiprows=3)
            
            df_raw = df_raw[df_raw['일자'].notna()]
            df_raw = df_raw[~df_raw['일자'].astype(str).str.contains('소계|Subtotal|합계|Total|합 계', na=False)]
            
            df = pd.DataFrame()
            df['Guest_Name'] = 'OTB_SUMMARY_DATA'
            df['CheckIn'] = pd.to_datetime(df_raw['일자'], errors='coerce')
            df['Booking_Date'] = df['CheckIn']
            df['RN'] = pd.to_numeric(df_raw.iloc[:, 14], errors='coerce').fillna(0) # 합계-객실수
            df['Room_Revenue'] = pd.to_numeric(df_raw.iloc[:, 18], errors='coerce').fillna(0) # 합계-매출
            df['Total_Revenue'] = df['Room_Revenue']
            df['ADR'] = pd.to_numeric(df_raw.iloc[:, 16], errors='coerce').fillna(0) # 합계-객단가
            df['Segment'] = 'OTB_Summary' # 지표 계산용 라벨
            df['Account'] = 'General'
            df['Room_Type'] = 'Standard'
            df['Nat_Orig'] = 'KOR'
        else:
            # [기존 상세 리스트] 원본 로직 100% 유지
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

        # 공통 처리 로직 (생략 없음)
        df['ADR'] = df['ADR'].replace([np.inf, -np.inf], 0).fillna(0)
        df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0
        df['Snapshot_Date'] = datetime.now().strftime('%Y-%m-%d')
        df['Status'] = status
        
        df['CheckIn_dt'] = pd.to_datetime(df['CheckIn'], errors='coerce')
        df['Booking_dt'] = pd.to_datetime(df.get('Booking_Date', df['CheckIn']), errors='coerce')
        
        df['Stay_Month'] = df['CheckIn_dt'].dt.strftime('%Y-%m')
        df['Stay_YearWeek'] = df['CheckIn_dt'].dt.strftime('%Y-%U주')
        df['Lead_Time'] = (df['CheckIn_dt'] - df['Booking_dt']).dt.days.fillna(0).astype(int)
        df['Day_of_Week'] = df['CheckIn_dt'].dt.day_name()
        
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
        if 'Booking_Date' in df.columns:
            df['Booking_Date'] = df['Booking_dt'].dt.strftime('%Y-%m-%d')

        return df[['Guest_Name', 'CheckIn', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Nat_Group', 'Month_Label', 'Is_Zero_Rate']]
    except Exception as e:
        st.error(f"🚨 파일 처리 중 치명적 오류: {e}")
        return pd.DataFrame()

# 3. 상세 분석 렌더링 (원본 무삭제 복구)
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
st.set_page_config(page_title="ARI Executive Pro", layout="wide")

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

    st.header("🏛️ 앰버 호텔 경영 리포트 (ARI Extreme)")

    # 데이터 로드
    raw_db = db_sheet.get_all_values()
    if len(raw_db) > 1:
        df = pd.DataFrame(raw_db[1:], columns=raw_db[0])
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Is_Zero_Rate 에러 방지
        if 'Is_Zero_Rate' not in df.columns:
            df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0
        else:
            df['Is_Zero_Rate'] = df['Is_Zero_Rate'].map({'True': True, 'False': False, True: True, False: False})

        all_dates = sorted(df['Snapshot_Date'].unique(), reverse=True)
        sel_date = st.sidebar.selectbox("Snapshot 선택", ["전체 누적"] + all_dates)
        
        f_df = df if sel_date == "전체 누적" else df[df['Snapshot_Date'] <= sel_date]
        paid_df = f_df[f_df['Is_Zero_Rate'] == False]
        
        # [핵심] 상단 지표는 영업현황(OTB_Summary) 데이터 우선 사용
        otb_data = paid_df[(paid_df['Segment'] == 'OTB_Summary') & (paid_df['Status'] == 'Booked')]
        # 영업현황 데이터가 없으면 일반 예약 리스트에서 가져옴
        if otb_data.empty:
            otb_data = paid_df[paid_df['Status'] == 'Booked']
            
        curr_month = datetime.now().strftime('%Y-%m')

        # --- [최상단] 2종 버짓 대시보드 ---
        st.subheader(f"🎯 실시간 버짓 달성 현황 ({sel_date})")
        
        # A. 당월 실적
        m_otb = otb_data[otb_data['Stay_Month'] == curr_month]
        m_rev, m_rn = m_otb['Room_Revenue'].sum(), m_otb['RN'].sum()
        m_budget = budget_df[budget_df['Month'] == curr_month]['Budget'].sum()
        m_achieve = (m_rev / m_budget * 100) if m_budget > 0 else 0
        m_adr = (m_rev / m_rn) if m_rn > 0 else 0

        # B. 전체 누계
        t_rev, t_rn = otb_data['Room_Revenue'].sum(), otb_data['RN'].sum()
        t_budget = budget_df['Budget'].sum()
        t_achieve = (t_rev / t_budget * 100) if t_budget > 0 else 0
        t_adr = (t_rev / t_rn) if t_rn > 0 else 0

        colA, colB = st.columns(2)
        with colA:
            st.info(f"🗓️ {curr_month} 당월 버짓")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("달성률", f"{m_achieve:.1f} %", delta=f"T: {m_budget:,.0f}")
            c2.metric("객실매출", f"{m_rev:,.0f} 원")
            c3.metric("ADR", f"{m_adr:,.0f} 원")
            c4.metric("룸나잇", f"{m_rn:,.0f} RN")
        with colB:
            st.info("🌍 전체 기간 누적 버짓")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("전체 달성률", f"{t_achieve:.1f} %", delta=f"T: {t_budget:,.0f}")
            k2.metric("누적 매출", f"{t_rev:,.0f} 원")
            k3.metric("누적 ADR", f"{t_adr:,.0f} 원")
            k4.metric("누적 RN", f"{t_rn:,.0f} RN")
        
        st.divider()

        # --- 탭 구성 (기존 기능 100% 복구) ---
        tab_month, tab_week, tab_det, tab_zero, tab_up = st.tabs(["🗓️ 월별 분석", "📅 주별 분석", "📈 상세 분석", "🆓 0원 예약", "📤 데이터 업데이트"])
        
        with tab_month:
            m_sum = otb_data.groupby('Stay_Month').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
            m_res = pd.merge(m_sum, budget_df, left_on='Stay_Month', right_on='Month', how='left').fillna(0)
            m_res['달성률(%)'] = (m_res['Room_Revenue'] / m_res['Budget'] * 100).replace([np.inf, -np.inf], 0).round(1)
            st.table(m_res.style.format({'RN':'{:,}', 'Room_Revenue':'{:,}', 'Budget':'{:,}', '달성률(%)':'{}%'}))
        
        with tab_week:
            net_df = pd.concat([otb_data, f_df[f_df['Status'] == 'Cancelled'].assign(RN=lambda x: -x['RN'], Room_Revenue=lambda x: -x['Room_Revenue'])])
            w_sum = net_df.groupby('Stay_YearWeek').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
            st.plotly_chart(px.line(w_sum, x='Stay_YearWeek', y='Room_Revenue', markers=True, title="주별 순매출 추이"), use_container_width=True)

        with tab_det:
            # 상세 분석은 요약 데이터가 아닌 실제 예약 리스트(Segment != OTB_Summary)로 수행
            res_bk = paid_df[(paid_df['Segment'] != 'OTB_Summary') & (paid_df['Status'] == 'Booked')]
            res_cn = f_df[(f_df['Segment'] != 'OTB_Summary') & (f_df['Status'] == 'Cancelled')]
            t1, t2 = st.tabs(["✅ 유료 예약 상세", "❌ 취소 리스트 상세"])
            with t1: render_full_analysis(res_bk, "유료 예약")
            with t2: render_full_analysis(res_cn, "취소 내역")

        with tab_zero:
            st.subheader("🆓 0원 예약 목록 (체험단/VIP 등)")
            st.dataframe(f_df[f_df['Is_Zero_Rate'] == True][['Guest_Name', 'CheckIn', 'Account', 'Room_Type']])

        with tab_up:
            st.subheader("📤 데이터 통합 업데이트 (기존 데이터 유지)")
            c1, c2, c3 = st.columns(3)
            with c1:
                f1 = st.file_uploader("1️⃣ 신규 예약 리스트", type=['xlsx', 'csv'], key="f1")
                if f1 and st.button("신규 예약 반영"):
                    db_sheet.append_rows(process_data(f1, "Booked").fillna('').astype(str).values.tolist())
                    st.success("완료!")
            with c2:
                f2 = st.file_uploader("2️⃣ 취소 리스트", type=['xlsx', 'csv'], key="f2")
                if f2 and st.button("취소 내역 반영"):
                    db_sheet.append_rows(process_data(f2, "Cancelled").fillna('').astype(str).values.tolist())
                    st.success("완료!")
            with col3 if 'col3' in locals() else c3: # 버튼 배치 유지
                f3 = st.file_uploader("3️⃣ 영업현황(OTB Summary)", type=['xlsx', 'csv'], key="f3")
                if f3 and st.button("영업현황 반영"):
                    db_sheet.append_rows(process_data(f3, "Booked").fillna('').astype(str).values.tolist())
                    st.success("완료!")
    else:
        st.warning("📡 사이드바나 업로드 탭에서 데이터를 먼저 채워주세요.")

except Exception as e:
    st.error(f"🚨 시스템 오류: {e}")
