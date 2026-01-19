import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import time
from gspread.exceptions import APIError

# ------------------------------------------------------------------------------
# 0. 스타일 설정
# ------------------------------------------------------------------------------
st.set_page_config(page_title="ARI Final Correction", layout="wide")
st.markdown("""
<style>
    div[data-testid="stMetricValue"] { font-size: 24px !important; font-weight: bold; }
    button[data-baseweb="tab"] { font-size: 16px !important; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# 1. 구글 시트 연결 & 캐싱
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

@st.cache_data(ttl=600)
def load_data_from_sheet(_sheet_obj):
    max_retries = 5
    for i in range(max_retries):
        try:
            return _sheet_obj.get_all_values()
        except APIError as e:
            if e.response.status_code == 429:
                time.sleep((2 ** i) + 1)
                continue
            else:
                raise e
    return []

# ------------------------------------------------------------------------------
# 2. 데이터 처리 엔진
# ------------------------------------------------------------------------------
def normalize_and_map_columns(df):
    col_map = {}
    rules = {
        'CheckIn': ['checkin', 'check-in', 'arrival', '입실', '일자', 'date'],
        'Guest_Name': ['guest', 'name', 'customer', '고객', '투숙객', '성명'],
        'Booking_Date': ['booking', 'create', 'res', '예약', '생성'],
        'Rooms': ['room', 'qty', 'rmws', '객실수', '수량'],
        'Nights': ['night', 'los', '박수', '박'],
        'Room_Revenue': ['room_rev', 'revenue', 'roomrate', '객실료', '매출'],
        'Total_Revenue': ['total', 'amount', '총금액', '합계'],
        'Segment': ['segment', '세그먼트'], # Segment 명확화
        'Account': ['account', 'source', 'agent', '거래처', '에이전시'],
        'Room_Type': ['type', 'cat', '객실타입', '룸타입'],
        'Nat_Orig': ['nation', 'country', 'nat', '국적']
    }

    for original_col in df.columns:
        clean_col = str(original_col).lower().replace(" ", "").replace("_", "").replace("-", "")
        mapped = False
        for target_col, keywords in rules.items():
            for kw in keywords:
                if kw in clean_col:
                    if target_col == 'Room_Revenue' and 'total' in clean_col: continue
                    if target_col == 'Total_Revenue' and 'room' in clean_col and 'total' not in clean_col: continue
                    if target_col == 'CheckIn' and ('book' in clean_col or 'res' in clean_col): continue
                    
                    if target_col not in col_map.values():
                        col_map[original_col] = target_col
                        mapped = True
                        break
            if mapped: break
    return df.rename(columns=col_map)

def find_valid_header_row(df):
    for i, row in df.iterrows():
        row_str = " ".join(row.astype(str).values).lower()
        keywords = ['guest', 'name', 'check', 'date', 'room', '고객', '입실', '객실']
        if sum(1 for k in keywords if k in row_str) >= 2:
            df.columns = df.iloc[i]
            return df.iloc[i+1:].reset_index(drop=True)
    return df

def process_data(uploaded_file, status, sub_segment="General"):
    try:
        is_otb = "Sales on the Book" in uploaded_file.name or "영업 현황" in uploaded_file.name
        
        if uploaded_file.name.endswith('.csv'):
            df_raw = pd.read_csv(uploaded_file, header=None)
        else:
            df_raw = pd.read_excel(uploaded_file, header=None)

        if is_otb:
            # [영역 1: OTB 처리]
            df_raw = find_valid_header_row(df_raw)
            if '일자' in df_raw.columns: 
                df_raw = df_raw[~df_raw['일자'].astype(str).str.contains('소계|Subtotal|합계|Total', na=False)]
            elif df_raw.shape[1] > 0:
                df_raw = df_raw[~df_raw.iloc[:, 0].astype(str).str.contains('소계|Subtotal|합계|Total', na=False)]

            df = pd.DataFrame()
            df['Guest_Name'] = f'OTB_{sub_segment}_DATA'
            
            date_col = next((c for c in df_raw.columns if '일자' in str(c) or 'Date' in str(c)), df_raw.columns[0])
            df['CheckIn'] = pd.to_datetime(df_raw[date_col], errors='coerce')
            
            try:
                df['RN'] = pd.to_numeric(df_raw.iloc[:, -5], errors='coerce').fillna(0)
                df['Room_Revenue'] = pd.to_numeric(df_raw.iloc[:, -1], errors='coerce').fillna(0)
                df['ADR'] = pd.to_numeric(df_raw.iloc[:, -3], errors='coerce').fillna(0)
                df['Total_Revenue'] = df['Room_Revenue'] 
            except:
                df['RN'] = 0; df['Room_Revenue'] = 0; df['ADR'] = 0; df['Total_Revenue'] = 0

            df['Booking_Date'] = df['CheckIn']
            df['Segment'] = f'OTB_{sub_segment}'
            df['Account'] = 'OTB_Summary'
            df['Room_Type'] = 'Run of House'
            df['Nat_Orig'] = 'KOR'
            df['Lead_Time'] = 0
            
        else:
            # [영역 2: 상세 리스트 처리]
            df_raw = find_valid_header_row(df_raw)
            # 합계 행 삭제 (강력 필터)
            df_raw = df_raw[~df_raw.iloc[:, 0].astype(str).str.contains('합계|Total|소계|Subtotal', case=False, na=False)]
            
            df = normalize_and_map_columns(df_raw).copy()
            if 'Guest_Name' in df.columns:
                df = df[~df['Guest_Name'].astype(str).str.contains('합계|Total|소계|Subtotal', case=False, na=False)]
            
            if 'CheckIn' not in df.columns: return pd.DataFrame()
            if 'Booking_Date' not in df.columns: df['Booking_Date'] = df['CheckIn']
            
            req_cols = ['Rooms', 'Nights', 'Room_Revenue', 'Total_Revenue', 'Guest_Name', 'Segment', 'Account', 'Room_Type', 'Nat_Orig']
            for c in req_cols:
                if c not in df.columns: 
                    if c in ['Rooms', 'Nights', 'Room_Revenue', 'Total_Revenue']: df[c] = 0 
                    else: df[c] = 'Unknown'

            for col in ['Room_Revenue', 'Total_Revenue', 'Rooms', 'Nights']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            df['Total_Revenue'] = np.where(df['Total_Revenue'] == 0, df['Room_Revenue'], df['Total_Revenue'])
            df['RN'] = df['Rooms'] * df['Nights'].replace(0, 1)
            
            # 0원 예약 판단 (Comp)
            df['Is_Zero_Rate'] = df['Room_Revenue'] <= 0
            
            # ADR (0원 제외 전 전체 계산용)
            df['ADR'] = df.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)

        # 공통 처리
        df['Snapshot_Date'] = datetime.now().strftime('%Y-%m-%d')
        df['Status'] = status
        
        # 날짜 파싱
        df['CheckIn_dt'] = pd.to_datetime(df['CheckIn'], errors='coerce')
        df['Booking_dt'] = pd.to_datetime(df['Booking_Date'], errors='coerce')
        # Booking Date 오류 시 CheckIn으로 대체
        df.loc[df['Booking_dt'].isna(), 'Booking_dt'] = df.loc[df['Booking_dt'].isna(), 'CheckIn_dt']
        
        df = df.dropna(subset=['CheckIn_dt'])

        df['Stay_Month'] = df['CheckIn_dt'].dt.strftime('%Y-%m')
        df['Booking_Month'] = df['Booking_dt'].dt.strftime('%Y-%m')
        df['Stay_YearWeek'] = df['CheckIn_dt'].dt.strftime('%Y-%U주')
        df['Day_of_Week'] = df['CheckIn_dt'].dt.day_name()
        
        df['Weekday_Num'] = df['CheckIn_dt'].dt.weekday
        df['Day_Type'] = df['Weekday_Num'].apply(lambda x: 'Weekend' if x >= 4 else 'Weekday')

        # [지배인님 강력 요청] 리드타임 강제 계산 (파일 값 무시)
        # 공식: 입실일 - 예약일
        df['Lead_Time'] = (df['CheckIn_dt'] - df['Booking_dt']).dt.days.fillna(0).astype(int)
        # 마이너스는 0으로 처리 (당일 예약 시간차 등)
        df['Lead_Time'] = df['Lead_Time'].apply(lambda x: 0 if x < 0 else x)
        
        def classify_nat(row):
            name = str(row.get('Guest_Name', ''))
            orig = str(row.get('Nat_Orig', '')).upper()
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
                else: return "3.그외"
            except: return "Unknown"
        df['Month_Label'] = df['CheckIn_dt'].apply(get_month_label)

        df['CheckIn'] = df['CheckIn_dt'].dt.strftime('%Y-%m-%d')
        
        cols = ['Guest_Name', 'CheckIn', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Status', 'Stay_Month', 'Booking_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_Type', 'Day_of_Week', 'Nat_Group', 'Month_Label', 'Is_Zero_Rate']
        
        final_df = pd.DataFrame()
        for c in cols:
            final_df[c] = df[c] if c in df.columns else ''
        return final_df

    except Exception as e:
        return pd.DataFrame()

# ------------------------------------------------------------------------------
# 3. 공통 차트 렌더러 (예약/취소 동일 분석용)
# ------------------------------------------------------------------------------
def render_analysis_tabs(target_df, title_prefix):
    # 탭 구성: 세그먼트, 패턴, 거래처, 리드타임, 객실타입, 요일
    t1, t2, t3, t4, t5, t6 = st.tabs([
        "📊 세그먼트", "📅 예약패턴(Pacing)", "🏢 거래처", 
        "⏳ 리드타임", "🛏️ 객실타입", "🗓️ 요일별"
    ])
    
    with t1:
        st.subheader(f"📊 {title_prefix} 세그먼트 분석")
        seg_stats = target_df.groupby('Segment').agg({'RN': 'sum', 'Room_Revenue': 'sum'}).reset_index()
        seg_stats['ADR'] = (seg_stats['Room_Revenue'] / seg_stats['RN']).fillna(0)
        
        c1, c2 = st.columns(2)
        c1.plotly_chart(px.pie(seg_stats, values='Room_Revenue', names='Segment', title="매출 비중"), use_container_width=True)
        c2.plotly_chart(px.bar(seg_stats, x='Segment', y='ADR', title="ADR 비교", text_auto=',.0f', color='Segment'), use_container_width=True)
        
        st.dataframe(seg_stats.sort_values('Room_Revenue', ascending=False),
                     column_config={"Room_Revenue": st.column_config.NumberColumn("매출", format="%d원"), "ADR": st.column_config.NumberColumn("ADR", format="%d원")}, hide_index=True, use_container_width=True)

    with t2:
        st.subheader(f"📅 {title_prefix} Pacing (예약월 vs 입실월)")
        pacing = target_df.pivot_table(index='Booking_Month', columns='Stay_Month', values='RN', aggfunc='sum', fill_value=0)
        fig = px.imshow(pacing, text_auto=True, aspect="auto", color_continuous_scale="Reds" if "취소" in title_prefix else "Blues")
        st.plotly_chart(fig, use_container_width=True)

    with t3:
        st.subheader(f"🏢 {title_prefix} 거래처 분석")
        acc_stats = target_df.groupby('Account').agg({'RN': 'sum', 'Room_Revenue': 'sum'}).reset_index()
        acc_stats['ADR'] = (acc_stats['Room_Revenue'] / acc_stats['RN']).fillna(0)
        
        fig_acc = px.scatter(acc_stats, x="RN", y="ADR", size="Room_Revenue", color="Account", hover_name="Account", size_max=60)
        st.plotly_chart(fig_acc, use_container_width=True)
        st.dataframe(acc_stats.sort_values('RN', ascending=False), 
                     column_config={"Room_Revenue": st.column_config.NumberColumn(format="%d원"), "ADR": st.column_config.NumberColumn(format="%d원")}, hide_index=True, use_container_width=True)

    with t4:
        st.subheader(f"⏳ {title_prefix} 리드타임 분석")
        bins = [-1, 0, 3, 7, 14, 30, 60, 90, 999]
        labels = ['당일', '1-3일', '4-7일', '8-14일', '15-30일', '31-60일', '61-90일', '90일+']
        target_df['Lead_Group'] = pd.cut(target_df['Lead_Time'], bins=bins, labels=labels)
        
        lead_stats = target_df.groupby('Lead_Group').agg({'RN': 'sum', 'Room_Revenue': 'sum'}).reset_index()
        lead_stats['ADR'] = (lead_stats['Room_Revenue'] / lead_stats['RN']).fillna(0)
        
        fig_lead = go.Figure()
        fig_lead.add_trace(go.Bar(x=lead_stats['Lead_Group'], y=lead_stats['RN'], name='RN', marker_color='red' if "취소" in title_prefix else 'blue'))
        fig_lead.add_trace(go.Scatter(x=lead_stats['Lead_Group'], y=lead_stats['ADR'], name='ADR', yaxis='y2', line=dict(color='black', width=2)))
        fig_lead.update_layout(yaxis2=dict(overlaying='y', side='right', title='ADR'), title="리드타임별 물량 vs 단가")
        st.plotly_chart(fig_lead, use_container_width=True)

    with t5:
        st.subheader(f"🛏️ {title_prefix} 객실타입 분석")
        rt_stats = target_df.groupby('Room_Type').agg({'RN': 'sum', 'Room_Revenue': 'sum'}).reset_index()
        rt_stats['ADR'] = (rt_stats['Room_Revenue'] / rt_stats['RN']).fillna(0)
        st.dataframe(rt_stats.sort_values('RN', ascending=False), 
                     column_config={"Room_Revenue": st.column_config.NumberColumn(format="%d원"), "ADR": st.column_config.NumberColumn(format="%d원")}, hide_index=True, use_container_width=True)

    with t6:
        st.subheader(f"🗓️ {title_prefix} 요일별 분석")
        wd_stats = target_df.groupby('Day_Type').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
        wd_stats['ADR'] = (wd_stats['Room_Revenue'] / wd_stats['RN']).fillna(0)
        
        c1, c2 = st.columns(2)
        c1.plotly_chart(px.bar(wd_stats, x='Day_Type', y='ADR', title="요일별 ADR", text_auto=',.0f'), use_container_width=True)
        c2.plotly_chart(px.pie(wd_stats, values='RN', names='Day_Type', title="요일별 비중"), use_container_width=True)

# ------------------------------------------------------------------------------
# UI 메인
# ------------------------------------------------------------------------------
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

    st.title("🏛️ 앰버 호텔 경영 리포트 (Final Correction)")

    # 초기화 및 업로드
    with st.sidebar.expander("🛠️ 데이터 관리", expanded=True):
        if st.button("🗑️ 전체 데이터 삭제 (필수)"):
            db_sheet.clear()
            cols = ['Guest_Name', 'CheckIn', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Status', 'Stay_Month', 'Booking_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_Type', 'Day_of_Week', 'Nat_Group', 'Month_Label', 'Is_Zero_Rate']
            db_sheet.append_row(cols)
            load_data_from_sheet.clear()
            st.success("초기화 완료!")
            time.sleep(1)
            st.rerun()

    st.sidebar.header("📤 데이터 업로드")
    
    # 1. 상세 리스트 (예약/취소)
    with st.sidebar.expander("📝 상세 리스트 (인사이트용)", expanded=False):
        f1 = st.file_uploader("신규 예약 리스트", type=['xlsx','csv'], key="f1")
        if f1 and st.button("신규 예약 반영"):
            df = process_data(f1, "Booked")
            if not df.empty:
                db_sheet.append_rows(df.fillna('').astype(str).values.tolist())
                load_data_from_sheet.clear()
                st.success("반영 완료!")
                time.sleep(2)
                st.rerun()
        
        f2 = st.file_uploader("취소 리스트", type=['xlsx','csv'], key="f2")
        if f2 and st.button("취소 리스트 반영"):
            # [중요] 취소 리스트는 Status="Cancelled"로 강제
            df = process_data(f2, "Cancelled")
            if not df.empty:
                db_sheet.append_rows(df.fillna('').astype(str).values.tolist())
                load_data_from_sheet.clear()
                st.success("반영 완료!")
                time.sleep(2)
                st.rerun()

    # 2. OTB
    with st.sidebar.expander("🎯 세일즈 온더북 (버짓용)", expanded=True):
        f3 = st.file_uploader("당월 OTB", type=['xlsx','csv'], key="f3")
        if f3 and st.button("당월 OTB 반영"):
            df = process_data(f3, "Booked", "Month")
            if not df.empty:
                db_sheet.append_rows(df.fillna('').astype(str).values.tolist())
                load_data_from_sheet.clear()
                st.success("반영 완료!")
                time.sleep(2)
                st.rerun()
        
        f4 = st.file_uploader("전체 OTB", type=['xlsx','csv'], key="f4")
        if f4 and st.button("전체 OTB 반영"):
            df = process_data(f4, "Booked", "Total")
            if not df.empty:
                db_sheet.append_rows(df.fillna('').astype(str).values.tolist())
                load_data_from_sheet.clear()
                st.success("반영 완료!")
                time.sleep(2)
                st.rerun()

    # 데이터 로드
    raw_data = load_data_from_sheet(db_sheet)
    if len(raw_data) <= 1:
        st.warning("⚠️ 데이터가 없습니다. 파일을 업로드해주세요.")
    else:
        df = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        
        # 수치 변환
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Lead_Time']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # 자가 복구 로직 (Self-Healing)
        if 'Booking_Date' not in df.columns: df['Booking_Date'] = df['CheckIn']
        df['Booking_dt'] = pd.to_datetime(df['Booking_Date'], errors='coerce')
        df['CheckIn_dt'] = pd.to_datetime(df['CheckIn'], errors='coerce')
        df.loc[df['Booking_dt'].isna(), 'Booking_dt'] = df.loc[df['Booking_dt'].isna(), 'CheckIn_dt']
        
        # [리드타임 2차 검증 - 여기서도 강제 계산]
        df['Lead_Time'] = (df['CheckIn_dt'] - df['Booking_dt']).dt.days.fillna(0).astype(int)
        df['Lead_Time'] = df['Lead_Time'].apply(lambda x: 0 if x < 0 else x)
        
        df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0
        df['Stay_Month'] = df['CheckIn_dt'].dt.strftime('%Y-%m')
        df['Booking_Month'] = df['Booking_dt'].dt.strftime('%Y-%m')
        
        # 데이터 분리
        all_snapshots = sorted(df['Snapshot_Date'].unique(), reverse=True)
        sel_snapshot = st.sidebar.selectbox("기준일(Snapshot)", ["전체 누적"] + all_snapshots)
        
        if sel_snapshot != "전체 누적":
            df = df[df['Snapshot_Date'] <= sel_snapshot]
            
        df_otb_m = df[df['Segment'] == 'OTB_Month']
        df_otb_t = df[df['Segment'] == 'OTB_Total']
        
        df_list = df[~df['Segment'].str.contains('OTB')]
        
        # [핵심] 유료 예약 / 취소 예약 분리
        df_paid_bk = df_list[(df_list['Status'] == 'Booked') & (df_list['Is_Zero_Rate'] == False)]
        df_zero_bk = df_list[(df_list['Status'] == 'Booked') & (df_list['Is_Zero_Rate'] == True)]
        df_list_cn = df_list[df_list['Status'] == 'Cancelled'] # 취소 데이터

        curr_month = datetime.now().strftime('%Y-%m')

        # ======================================================================
        # [영역 1] OTB 버짓
        # ======================================================================
        st.markdown("### 🎯 세일즈 온더북 버짓 달성현황 (Source: OTB File)")
        
        # 당월
        if not df_otb_m.empty:
            m_rev = df_otb_m['Room_Revenue'].sum()
            m_trev = df_otb_m['Total_Revenue'].sum()
            m_rn = df_otb_m['RN'].sum()
            m_adr = (m_rev / m_rn) if m_rn > 0 else 0
        else:
            m_rev = 0; m_trev = 0; m_rn = 0; m_adr = 0
            
        m_budget = budget_df[budget_df['Month'] == curr_month]['Budget'].sum()
        m_achieve = (m_rev / m_budget * 100) if m_budget > 0 else 0

        # 전체
        if not df_otb_t.empty:
            t_rev = df_otb_t['Room_Revenue'].sum()
            t_trev = df_otb_t['Total_Revenue'].sum()
            t_rn = df_otb_t['RN'].sum()
            t_adr = (t_rev / t_rn) if t_rn > 0 else 0
        else:
            t_rev = 0; t_trev = 0; t_rn = 0; t_adr = 0
            
        t_budget = budget_df['Budget'].sum()
        t_achieve = (t_rev / t_budget * 100) if t_budget > 0 else 0

        c1, c2 = st.columns(2)
        with c1:
            st.info(f"📅 {curr_month} 당월 (객실매출 기준)")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("달성률", f"{m_achieve:.1f}%")
            col2.metric("객실매출", f"{m_rev:,.0f}")
            col3.metric("총매출", f"{m_trev:,.0f}")
            col4.metric("ADR / RN", f"{m_adr:,.0f} / {m_rn:,.0f}")
            st.caption(f"목표: {m_budget:,.0f}원")

        with c2:
            st.success("🌍 전체 누적 (객실매출 기준)")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("달성률", f"{t_achieve:.1f}%")
            k2.metric("객실매출", f"{t_rev:,.0f}")
            k3.metric("총매출", f"{t_trev:,.0f}")
            k4.metric("ADR / RN", f"{t_adr:,.0f} / {t_rn:,.0f}")
            st.caption(f"목표: {t_budget:,.0f}원")

        st.divider()

        # ======================================================================
        # [영역 2] 상세 인사이트 (예약 vs 취소 탭 구조)
        # ======================================================================
        st.markdown("### 📊 예약/취소 상세 인사이트 (Source: List File)")
        
        if df_list.empty:
            st.warning("데이터가 없습니다.")
        else:
            # [구조 변경] 3개의 메인 탭으로 나눔
            main_tabs = st.tabs(["✅ 예약 데이터 분석", "❌ 취소 데이터 분석", "📈 종합 합계"])
            
            # --- 1. 예약 분석 탭 ---
            with main_tabs[0]:
                st.caption("※ 0원(무료) 예약은 제외된 '유료 예약' 기준 분석입니다.")
                render_analysis_tabs(df_paid_bk, "유료 예약")
                
                st.divider()
                with st.expander("🆓 0원 예약 데이터 (격리됨)"):
                    st.write(f"총 {len(df_zero_bk)}건")
                    st.dataframe(df_zero_bk[['Guest_Name', 'CheckIn', 'Account', 'Room_Type']], use_container_width=True)

            # --- 2. 취소 분석 탭 ---
            with main_tabs[1]:
                st.caption("※ 취소된 예약에 대한 원인 및 패턴 분석입니다.")
                if not df_list_cn.empty:
                    render_analysis_tabs(df_list_cn, "취소 내역")
                else:
                    st.info("취소 데이터가 없습니다.")

            # --- 3. 종합 합계 탭 ---
            with main_tabs[2]:
                st.subheader("📈 리스트 데이터 합계 (유료 예약 기준)")
                c1, c2, c3 = st.columns(3)
                c1.metric("총 객실수 (RN)", f"{df_paid_bk['RN'].sum():,.0f} RN")
                c2.metric("총 객실매출", f"{df_paid_bk['Room_Revenue'].sum():,.0f} 원")
                c3.metric("총 매출 (부대포함)", f"{df_paid_bk['Total_Revenue'].sum():,.0f} 원")
                
                st.divider()
                st.subheader("📉 취소 데이터 합계")
                cc1, cc2 = st.columns(2)
                cc1.metric("총 취소 RN", f"{df_list_cn['RN'].sum():,.0f} RN")
                cc2.metric("총 취소 금액(객실)", f"{df_list_cn['Room_Revenue'].sum():,.0f} 원")

except Exception as e:
    st.error(f"🚨 시스템 오류: {e}")
