import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime
import plotly.express as px
import numpy as np
import time  # [핵심] 새로고침 딜레이용

# ------------------------------------------------------------------------------
# 1. 구글 시트 연결
# ------------------------------------------------------------------------------
def get_gspread_client():
    try:
        creds_info = st.secrets["gcp_service_account"]
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ 인증 오류: {e}")
        return None

# ------------------------------------------------------------------------------
# 2. 데이터 처리 엔진 (안전장치 강화)
# ------------------------------------------------------------------------------
def process_data(uploaded_file, status, sub_segment="General"):
    try:
        is_otb = "Sales on the Book" in uploaded_file.name or "영업 현황" in uploaded_file.name
        
        if is_otb:
            if uploaded_file.name.endswith('.csv'):
                df_raw = pd.read_csv(uploaded_file, skiprows=3)
            else:
                df_raw = pd.read_excel(uploaded_file, skiprows=3)
            
            # 소계, 합계 제거
            df_raw = df_raw[df_raw['일자'].notna()]
            df_raw = df_raw[~df_raw['일자'].astype(str).str.contains('소계|Subtotal|합계|Total|합 계', na=False)]
            
            df = pd.DataFrame()
            df['Guest_Name'] = f'OTB_{sub_segment}_DATA'
            df['CheckIn'] = pd.to_datetime(df_raw['일자'], errors='coerce')
            
            # 맨 뒤에서부터 컬럼 가져오기 (안전책)
            # 끝=매출, 끝-2=ADR, 끝-4=객실수 (파일 구조에 따라 유동적 대응)
            df['RN'] = pd.to_numeric(df_raw.iloc[:, 14], errors='coerce').fillna(0) # 14: 객실수
            df['Room_Revenue'] = pd.to_numeric(df_raw.iloc[:, 18], errors='coerce').fillna(0) # 18: 매출
            df['Total_Revenue'] = df['Room_Revenue']
            df['ADR'] = pd.to_numeric(df_raw.iloc[:, 16], errors='coerce').fillna(0) # 16: ADR
            
            df['Booking_Date'] = df['CheckIn']
            df['Segment'] = f'OTB_{sub_segment}'
            df['Account'] = 'OTB_Summary'
            df['Room_Type'] = 'Run of House'
            df['Nat_Orig'] = 'KOR'
            
        else:
            if uploaded_file.name.endswith('.csv'):
                df_raw = pd.read_csv(uploaded_file, skiprows=1)
            else:
                df_raw = pd.read_excel(uploaded_file, skiprows=1)
            
            df_raw.columns = df_raw.iloc[0]
            df_raw = df_raw.drop(df_raw.index[0]).reset_index(drop=True)
            df_raw = df_raw[df_raw['고객명'].notna()]
            df_raw = df_raw[~df_raw['고객명'].astype(str).str.contains('합계|Total', na=False)]
            
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

        # 공통 후처리
        df['ADR'] = df['ADR'].replace([np.inf, -np.inf], 0).fillna(0)
        df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0 
        df['Snapshot_Date'] = datetime.now().strftime('%Y-%m-%d')
        df['Status'] = status
        
        df['CheckIn_dt'] = pd.to_datetime(df['CheckIn'], errors='coerce')
        df['Booking_dt'] = pd.to_datetime(df.get('Booking_Date', df['CheckIn']), errors='coerce')
        
        df['Stay_Month'] = df['CheckIn_dt'].dt.strftime('%Y-%m')
        df['Stay_YearWeek'] = df['CheckIn_dt'].dt.strftime('%Y-%U주')
        df['Day_of_Week'] = df['CheckIn_dt'].dt.day_name()
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

        cols = ['Guest_Name', 'CheckIn', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Nat_Group', 'Month_Label', 'Is_Zero_Rate']
        
        for c in cols:
            if c not in df.columns: df[c] = ''
            
        return df[cols]

    except Exception as e:
        st.error(f"🚨 데이터 처리 중 오류 발생: {e}")
        return pd.DataFrame()

def render_full_analysis(data, title):
    if data is None or data.empty:
        st.info(f"📍 {title} 데이터가 없습니다.")
        return
    st.markdown(f"#### 📊 {title} 분석")
    c1, c2 = st.columns(2)
    with c1:
        st.write("**🏢 거래처별**")
        acc = data.groupby('Account').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        acc['ADR'] = (acc['Room_Revenue'] / acc['RN']).fillna(0).astype(int)
        st.table(acc.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))
    with c2:
        st.write("**🛏️ 객실타입별**")
        rt = data.groupby('Room_Type').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        rt['ADR'] = (rt['Room_Revenue'] / rt['RN']).fillna(0).astype(int)
        st.table(rt.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))

# ------------------------------------------------------------------------------
# UI 메인
# ------------------------------------------------------------------------------
st.set_page_config(page_title="ARI Extreme Master", layout="wide")

try:
    c = get_gspread_client()
    sh = c.open("Amber_Revenue_DB")
    db_sheet = sh.get_worksheet(0)
    
    try:
        budget_raw = sh.worksheet("Budget").get_all_values()
        budget_df = pd.DataFrame(budget_raw[1:], columns=budget_raw[0])
        budget_df['Budget'] = pd.to_numeric(budget_df['Budget'], errors='coerce').fillna(0)
    except:
        budget_df = pd.DataFrame(columns=['Month', 'Budget'])

    st.title("🏛️ 앰버 호텔 경영 리포트 (ARI Extreme)")

    # --------------------------------------------------------------------------
    # 1. 사이드바 - 4종 개별 업로드 (자동 새로고침 기능 탑재)
    # --------------------------------------------------------------------------
    st.sidebar.header("📤 데이터 업로드 센터")
    
    with st.sidebar.expander("📝 1. 신규 예약 리스트", expanded=False):
        f1 = st.file_uploader("신규 예약 파일", type=['xlsx','csv'], key="f1")
        if f1 and st.button("신규 예약 반영"):
            df_new = process_data(f1, "Booked")
            if not df_new.empty:
                db_sheet.append_rows(df_new.fillna('').astype(str).values.tolist())
                st.success("반영 완료! (화면을 갱신합니다...)")
                time.sleep(1) # 시트 저장 대기
                st.rerun()    # [핵심] 화면 강제 새로고침

    with st.sidebar.expander("❌ 2. 취소 리스트", expanded=False):
        f2 = st.file_uploader("취소 리스트 파일", type=['xlsx','csv'], key="f2")
        if f2 and st.button("취소 내역 반영"):
            df_cn = process_data(f2, "Cancelled")
            if not df_cn.empty:
                db_sheet.append_rows(df_cn.fillna('').astype(str).values.tolist())
                st.success("반영 완료! (화면을 갱신합니다...)")
                time.sleep(1)
                st.rerun()

    with st.sidebar.expander("🗓️ 3. 영업현황 (당월 전용)", expanded=True):
        f3 = st.file_uploader("당월 OTB 파일", type=['xlsx','csv'], key="f3")
        if f3 and st.button("당월 OTB 반영"):
            df_m = process_data(f3, "Booked", "Month")
            if not df_m.empty:
                db_sheet.append_rows(df_m.fillna('').astype(str).values.tolist())
                st.success("반영 완료! (화면을 갱신합니다...)")
                time.sleep(1)
                st.rerun()

    with st.sidebar.expander("🌍 4. 영업현황 (전체 누적)", expanded=True):
        f4 = st.file_uploader("전체 OTB 파일", type=['xlsx','csv'], key="f4")
        if f4 and st.button("전체 OTB 반영"):
            df_t = process_data(f4, "Booked", "Total")
            if not df_t.empty:
                db_sheet.append_rows(df_t.fillna('').astype(str).values.tolist())
                st.success("반영 완료! (화면을 갱신합니다...)")
                time.sleep(1)
                st.rerun()

    # --------------------------------------------------------------------------
    # 2. 데이터 로드 및 분석
    # --------------------------------------------------------------------------
    raw_data = db_sheet.get_all_values()
    if len(raw_data) > 1:
        df = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        
        # [중요] 수치형 강제 변환
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        # [중요] Is_Zero_Rate 재계산 (DB값 무시하고 즉석 계산으로 충돌 방지)
        df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0
        
        all_snapshots = sorted(df['Snapshot_Date'].unique(), reverse=True)
        sel_snapshot = st.sidebar.selectbox("Snapshot 선택", ["전체 누적"] + all_snapshots)
        
        # 스냅샷 필터링
        if sel_snapshot != "전체 누적":
            df = df[df['Snapshot_Date'] <= sel_snapshot]
            
        # 유료 데이터 (대시보드용)
        paid_df = df[~df['Is_Zero_Rate']].copy()
        
        curr_month = datetime.now().strftime('%Y-%m')

        # ----------------------------------------------------------------------
        # 3. 2종 버짓 대시보드
        # ----------------------------------------------------------------------
        st.subheader(f"🎯 실시간 버짓 달성 현황 (기준: {sel_snapshot})")
        
        # A. 당월 (OTB_Month 우선)
        otb_m = paid_df[(paid_df['Segment'] == 'OTB_Month') & (paid_df['Status'] == 'Booked')]
        if otb_m.empty: # 없으면 일반 리스트에서 계산
            otb_m = paid_df[(paid_df['Status'] == 'Booked') & (paid_df['Stay_Month'] == curr_month)]
            
        m_rev = otb_m['Room_Revenue'].sum()
        m_rn = otb_m['RN'].sum()
        m_adr = (m_rev / m_rn) if m_rn > 0 else 0
        m_budget = budget_df[budget_df['Month'] == curr_month]['Budget'].sum()
        m_achieve = (m_rev / m_budget * 100) if m_budget > 0 else 0

        # B. 전체 (OTB_Total 우선)
        otb_t = paid_df[(paid_df['Segment'] == 'OTB_Total') & (paid_df['Status'] == 'Booked')]
        if otb_t.empty: # 없으면 전체 리스트에서 계산
            otb_t = paid_df[paid_df['Status'] == 'Booked']
            
        t_rev = otb_t['Room_Revenue'].sum()
        t_rn = otb_t['RN'].sum()
        t_adr = (t_rev / t_rn) if t_rn > 0 else 0
        t_budget = budget_df['Budget'].sum()
        t_achieve = (t_rev / t_budget * 100) if t_budget > 0 else 0

        col1, col2 = st.columns(2)
        with col1:
            st.info(f"🗓️ {curr_month} 당월 실적")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("달성률", f"{m_achieve:.1f}%", delta=f"T:{m_budget:,.0f}")
            m2.metric("매출", f"{m_rev:,.0f}")
            m3.metric("ADR", f"{m_adr:,.0f}")
            m4.metric("RN", f"{m_rn:,.0f}")
            
        with col2:
            st.info("🌍 전체 누적 실적")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("달성률", f"{t_achieve:.1f}%", delta=f"T:{t_budget:,.0f}")
            k2.metric("매출", f"{t_rev:,.0f}")
            k3.metric("ADR", f"{t_adr:,.0f}")
            k4.metric("RN", f"{t_rn:,.0f}")

        st.divider()

        # ----------------------------------------------------------------------
        # 4. 분석 탭 (원본 100% 복구)
        # ----------------------------------------------------------------------
        t1, t2, t3, t4 = st.tabs(["🗓️ 월별 분석", "📅 주별 추이", "📈 상세 리포트", "🆓 0원 예약"])
        
        with t1:
            # 월별 데이터: 예산 대비 달성률 확인
            monthly = otb_t.groupby('Stay_Month').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
            monthly = pd.merge(monthly, budget_df, left_on='Stay_Month', right_on='Month', how='left').fillna(0)
            monthly['달성률(%)'] = (monthly['Room_Revenue'] / monthly['Budget'] * 100).replace([np.inf, -np.inf], 0).round(1)
            st.table(monthly.style.format({'RN':'{:,}', 'Room_Revenue':'{:,}', 'Budget':'{:,}', '달성률(%)':'{}%'}))
            
        with t2:
            # 주별 데이터: OTB 전체 + 취소 내역 합산
            cn_df = df[(df['Status'] == 'Cancelled') & (df['Segment'].str.contains('OTB') == False)]
            cn_df = cn_df.assign(RN = -cn_df['RN'], Room_Revenue = -cn_df['Room_Revenue'])
            
            combined = pd.concat([otb_t, cn_df])
            weekly = combined.groupby('Stay_YearWeek').agg({'Room_Revenue':'sum'}).reset_index()
            st.plotly_chart(px.line(weekly, x='Stay_YearWeek', y='Room_Revenue', markers=True, title="주별 순매출 추이"), use_container_width=True)
            
        with t3:
            # 상세 분석: 순수 예약 리스트(OTB 제외)로만
            pure_bk = paid_df[(paid_df['Segment'].str.contains('OTB') == False) & (paid_df['Status'] == 'Booked')]
            pure_cn = df[(df['Segment'].str.contains('OTB') == False) & (df['Status'] == 'Cancelled')]
            
            sub_t1, sub_t2 = st.tabs(["예약 상세", "취소 상세"])
            with sub_t1: render_full_analysis(pure_bk, "유료 예약")
            with sub_t2: render_full_analysis(pure_cn, "취소 내역")
            
        with t4:
            st.subheader("🆓 0원 예약 (체험단/VIP)")
            # 0원 예약은 순수 리스트에서 추출
            zero_df = df[(df['Is_Zero_Rate'] == True) & (df['Segment'].str.contains('OTB') == False)]
            st.dataframe(zero_df[['Guest_Name', 'CheckIn', 'Account', 'Room_Type']])

    else:
        st.warning("👈 사이드바에서 파일을 업로드해주세요.")

except Exception as e:
    st.error(f"🚨 시스템 오류: {e}")
