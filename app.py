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

# 2. 데이터 처리 엔진 (원본 18개 컬럼 유지 + OTB 합계 정밀 추출 + 소계 제거)
def process_data(uploaded_file, status, sub_segment="General"):
    try:
        # 파일 형식 판별 (영업현황 요약 vs 상세 리스트)
        is_otb = "Sales on the Book" in uploaded_file.name or "영업 현황" in uploaded_file.name
        
        if is_otb:
            # [영업현황] 4번째 줄부터 데이터 시작, 소계 제거, 오른쪽 합계 섹션 추출
            if uploaded_file.name.endswith('.csv'):
                df_raw = pd.read_csv(uploaded_file, skiprows=3)
            else:
                df_raw = pd.read_excel(uploaded_file, skiprows=3)
            
            # 소계 및 합계 행 완전 제거 (지표 중복 방지)
            df_raw = df_raw[df_raw['일자'].notna()]
            df_raw = df_raw[~df_raw['일자'].astype(str).str.contains('소계|Subtotal|합계|Total|합 계', na=False)]
            
            df = pd.DataFrame()
            df['Guest_Name'] = f'OTB_{sub_segment}_DATA'
            df['CheckIn'] = pd.to_datetime(df_raw['일자'], errors='coerce')
            
            # 합계 섹션 정밀 추출: 객실수(14), 객단가(16), 매출(18)
            df['RN'] = pd.to_numeric(df_raw.iloc[:, 14], errors='coerce').fillna(0)
            df['Room_Revenue'] = pd.to_numeric(df_raw.iloc[:, 18], errors='coerce').fillna(0)
            df['Total_Revenue'] = df['Room_Revenue']
            df['ADR'] = pd.to_numeric(df_raw.iloc[:, 16], errors='coerce').fillna(0)
            
            df['Booking_Date'] = df['CheckIn']
            df['Segment'] = f'OTB_{sub_segment}' # 당월/전체 구분용
            df['Account'] = 'General'
            df['Room_Type'] = 'Standard'
            df['Nat_Orig'] = 'KOR'
        else:
            # [상세 리스트] 원본 로직 100% 무삭제 유지
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

        # 공통 처리 로직 (원본 모든 기능 100% 복구)
        df['ADR'] = df['ADR'].replace([np.inf, -np.inf], 0).fillna(0)
        df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0
        df['Snapshot_Date'] = datetime.now().strftime('%Y-%m-%d')
        df['Status'] = status
        
        df['CheckIn_dt'] = pd.to_datetime(df['CheckIn'], errors='coerce')
        df['Stay_Month'] = df['CheckIn_dt'].dt.strftime('%Y-%m')
        df['Stay_YearWeek'] = df['CheckIn_dt'].dt.strftime('%Y-%U주')
        df['Day_of_Week'] = df['CheckIn_dt'].dt.day_name()
        
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
        if 'Booking_Date' in df.columns:
            df['Booking_Date'] = df['Booking_dt'].dt.strftime('%Y-%m-%d')

        return df[['Guest_Name', 'CheckIn', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Nat_Group', 'Month_Label', 'Is_Zero_Rate']]
    except Exception as e:
        st.error(f"🚨 파일 처리 중 치명적 오류: {e}")
        return pd.DataFrame()

# 3. 상세 분석 렌더링 함수
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
st.set_page_config(page_title="ARI Executive Pro Plus", layout="wide")

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

    st.header("🏛️ 앰버 호텔 경영 요약 리포트 (ARI Extreme)")

    # 1. 사이드바 - 업로드 버튼 4종 완전 분리 (지배인님 엄명)
    st.sidebar.subheader("📤 데이터 업로드 센터")
    
    with st.sidebar.expander("📝 1. 신규 예약 리스트 업로드", expanded=False):
        f_new = st.file_uploader("신규 파일 선택", type=['xlsx', 'csv'], key="up_new")
        if f_new and st.button("신규 예약 반영"):
            db_sheet.append_rows(process_data(f_new, "Booked").fillna('').astype(str).values.tolist())
            st.success("완료!")

    with st.sidebar.expander("❌ 2. 취소 리스트 업로드", expanded=False):
        f_cn = st.file_uploader("취소 파일 선택", type=['xlsx', 'csv'], key="up_cn")
        if f_cn and st.button("취소 내역 반영"):
            db_sheet.append_rows(process_data(f_cn, "Cancelled").fillna('').astype(str).values.tolist())
            st.success("완료!")

    with st.sidebar.expander("🗓️ 3. 영업현황 (당월 전용)", expanded=True):
        f_otb_m = st.file_uploader("당월 OTB 선택", type=['xlsx', 'csv'], key="up_m")
        if f_otb_m and st.button("당월 OTB 저장"):
            db_sheet.append_rows(process_data(f_otb_m, "Booked", "Month").fillna('').astype(str).values.tolist())
            st.success("완료!")

    with st.sidebar.expander("🌍 4. 영업현황 (전체 누적)", expanded=True):
        f_otb_t = st.file_uploader("전체 OTB 선택", type=['xlsx', 'csv'], key="up_t")
        if f_otb_t and st.button("전체 OTB 저장"):
            db_sheet.append_rows(process_data(f_otb_t, "Booked", "Total").fillna('').astype(str).values.tolist())
            st.success("완료!")

    # 2. 데이터 로드 및 2종 버짓 대시보드
    raw_db = db_sheet.get_all_values()
    if len(raw_db) > 1:
        df = pd.DataFrame(raw_db[1:], columns=raw_db[0])
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # [에러 박멸] Is_Zero_Rate 타입 강제 보정 및 필터링
        df['Is_Zero_Rate'] = df['Is_Zero_Rate'].astype(str).str.upper().map({'TRUE': True, 'FALSE': False}).fillna(False)

        all_dates = sorted(df['Snapshot_Date'].unique(), reverse=True)
        sel_date = st.sidebar.selectbox("Snapshot 선택", ["전체 누적"] + all_dates)
        
        f_df = df if sel_date == "전체 누적" else df[df['Snapshot_Date'] <= sel_date]
        paid_df = f_df[f_df['Is_Zero_Rate'] == False]
        
        curr_month = datetime.now().strftime('%Y-%m')

        # 상단 KPI (영업현황 데이터 기반)
        otb_m = paid_df[(paid_df['Segment'] == 'OTB_Month') & (paid_df['Status'] == 'Booked')]
        m_rev, m_rn = otb_m['Room_Revenue'].sum(), otb_m['RN'].sum()
        m_budget = budget_df[budget_df['Month'] == curr_month]['Budget'].sum()
        m_achieve = (m_rev / m_budget * 100) if m_budget > 0 else 0

        otb_t = paid_df[(paid_df['Segment'] == 'OTB_Total') & (paid_df['Status'] == 'Booked')]
        t_rev, t_rn = otb_t['Room_Revenue'].sum(), otb_t['RN'].sum()
        t_budget = budget_df['Budget'].sum()
        t_achieve = (t_rev / t_budget * 100) if t_budget > 0 else 0

        st.subheader(f"🎯 실시간 버짓 달성 현황 ({sel_date})")
        colA, colB = st.columns(2)
        with colA:
            st.info(f"🗓️ {curr_month} 당월 버짓 현황")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("달성률", f"{m_achieve:.1f}%", delta=f"T:{m_budget:,.0f}")
            c2.metric("매출", f"{m_rev:,.0f} 원")
            c3.metric("ADR", f"{(m_rev/m_rn if m_rn>0 else 0):,.0f} 원"); c4.metric("RN", f"{m_rn:,.0f} RN")
        with colB:
            st.info("🌍 전체 기간 누적 버짓 현황")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("전체 달성률", f"{t_achieve:.1f}%", delta=f"T:{t_budget:,.0f}")
            k2.metric("누적 매출", f"{t_rev:,.0f} 원")
            k3.metric("누적 ADR", f"{(t_rev/t_rn if t_rn>0 else 0):,.0f} 원"); k4.metric("누적 RN", f"{t_rn:,.0f} RN")
        
        st.divider()

        # --- 탭 구성 (원본 로직 100% 무삭제 복구) ---
        tab_month, tab_week, tab_det, tab_zero = st.tabs(["🗓️ 월별 분석", "📅 주별 분석", "📈 상세 분석 리포트", "🆓 0원 예약 목록"])
        
        with tab_month:
            m_sum = otb_t.groupby('Stay_Month').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
            m_res = pd.merge(m_sum, budget_df, left_on='Stay_Month', right_on='Month', how='left').fillna(0)
            m_res['달성률(%)'] = (m_res['Room_Revenue'] / m_res['Budget'] * 100).replace([np.inf, -np.inf], 0).round(1)
            st.table(m_res.style.format({'RN':'{:,}', 'Room_Revenue':'{:,}', 'Budget':'{:,}', '달성률(%)':'{}%'}))
        
        with tab_week:
            # 주별 순매출 (OTB + 취소 반영)
            net_df = pd.concat([otb_t, f_df[f_df['Status'] == 'Cancelled'].assign(RN=lambda x: -x['RN'], Room_Revenue=lambda x: -x['Room_Revenue'])])
            w_sum = net_df.groupby('Stay_YearWeek').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
            st.plotly_chart(px.line(w_sum, x='Stay_YearWeek', y='Room_Revenue', markers=True, title="주별 순매출 추이"), use_container_width=True)

        with tab_det:
            detail_bk = paid_df[(~paid_df['Segment'].str.contains('OTB', na=False)) & (paid_df['Status'] == 'Booked')]
            detail_cn = f_df[(~f_df['Segment'].str.contains('OTB', na=False)) & (f_df['Status'] == 'Cancelled')]
            t1, t2 = st.tabs(["✅ 유료 예약 상세", "❌ 취소 리스트 상세"])
            with t1: render_full_analysis(detail_bk, "유료 예약 리스트")
            with t2: render_full_analysis(detail_cn, "취소 내역 리스트")

        with tab_zero:
            st.subheader("🆓 0원 예약 목록 (상세 리스트 기준)")
            st.dataframe(f_df[f_df['Is_Zero_Rate'] == True][['Guest_Name', 'CheckIn', 'Account', 'Room_Type']])
    else:
        st.warning("📡 사이드바에서 [당월 OTB] 또는 [전체 OTB] 파일을 먼저 업로드해 주세요.")

except Exception as e:
    st.error(f"🚨 시스템 오류: {e}")
