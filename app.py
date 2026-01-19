import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime
import plotly.express as px
import numpy as np
import time
from gspread.exceptions import APIError

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
        'Segment': ['segment', 'mkt', '시장'],
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
            # OTB 파일 내의 합계 행 제거
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
            except:
                df['RN'] = 0; df['Room_Revenue'] = 0; df['ADR'] = 0

            df['Total_Revenue'] = df['Room_Revenue']
            df['Booking_Date'] = df['CheckIn']
            df['Segment'] = f'OTB_{sub_segment}'
            df['Account'] = 'OTB_Summary'
            df['Room_Type'] = 'Run of House'
            df['Nat_Orig'] = 'KOR'
            
        else:
            # [영역 2: 상세 리스트 처리]
            df_raw = find_valid_header_row(df_raw)
            
            # -----------------------------------------------------------
            # [지배인님 요청 핵심 수정] 파일 자체의 맨 아래 '합계(Total)' 행 삭제 로직 강화
            # -----------------------------------------------------------
            # 1차 필터: 첫 번째 컬럼에서 필터링
            df_raw = df_raw[~df_raw.iloc[:, 0].astype(str).str.contains('합계|Total|소계|Subtotal', case=False, na=False)]
            
            df = normalize_and_map_columns(df_raw).copy()
            
            # 2차 필터: 매핑 후 'Guest_Name'이나 'CheckIn' 컬럼에서 한 번 더 필터링 (확실하게 죽임)
            if 'Guest_Name' in df.columns:
                df = df[~df['Guest_Name'].astype(str).str.contains('합계|Total|소계|Subtotal', case=False, na=False)]
            
            if 'CheckIn' not in df.columns: return pd.DataFrame()
            if 'Booking_Date' not in df.columns: df['Booking_Date'] = df['CheckIn']
            
            req_cols = ['Rooms', 'Nights', 'Room_Revenue', 'Total_Revenue', 'Guest_Name', 'Segment', 'Account', 'Room_Type', 'Nat_Orig']
            for c in req_cols:
                if c not in df.columns: df[c] = 0 if 'Revenue' in c or c in ['Rooms', 'Nights'] else ''

            for col in ['Room_Revenue', 'Total_Revenue', 'Rooms', 'Nights']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            df['RN'] = df['Rooms'] * df['Nights'].replace(0, 1)
            df['ADR'] = df.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)

        # 공통 후처리
        df['ADR'] = df['ADR'].replace([np.inf, -np.inf], 0).fillna(0)
        df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0 
        df['Snapshot_Date'] = datetime.now().strftime('%Y-%m-%d')
        df['Status'] = status
        
        df['CheckIn_dt'] = pd.to_datetime(df['CheckIn'], errors='coerce')
        df['Booking_dt'] = pd.to_datetime(df['Booking_Date'], errors='coerce')
        df.loc[df['Booking_dt'].isna(), 'Booking_dt'] = df.loc[df['Booking_dt'].isna(), 'CheckIn_dt']
        
        # 합계 행 삭제 후 남은 찌꺼기 데이터(날짜가 비어있는 행) 제거
        df = df.dropna(subset=['CheckIn_dt'])

        df['Stay_Month'] = df['CheckIn_dt'].dt.strftime('%Y-%m')
        df['Stay_YearWeek'] = df['CheckIn_dt'].dt.strftime('%Y-%U주')
        df['Day_of_Week'] = df['CheckIn_dt'].dt.day_name()
        df['Lead_Time'] = (df['CheckIn_dt'] - df['Booking_dt']).dt.days.fillna(0).astype(int)
        
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
        cols = ['Guest_Name', 'CheckIn', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Nat_Group', 'Month_Label', 'Is_Zero_Rate']
        
        final_df = pd.DataFrame()
        for c in cols:
            final_df[c] = df[c] if c in df.columns else ''
        return final_df

    except Exception as e:
        return pd.DataFrame()

# 상세 분석 렌더링 (숫자 포맷팅 적용: 콤마 및 원 표시)
def render_full_analysis(data, title):
    if data is None or data.empty:
        st.info(f"📍 {title} 데이터가 없습니다.")
        return
    
    st.markdown(f"#### 📊 {title} 심층 분석")
    
    c1, c2 = st.columns(2)
    with c1:
        st.caption("🏢 거래처별 실적")
        acc = data.groupby('Account').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        acc['ADR'] = (acc['Room_Revenue'] / acc['RN']).fillna(0).astype(int)
        # [NEW] 숫자 예쁘게 나오게 포맷팅
        st.dataframe(
            acc.sort_values('Room_Revenue', ascending=False),
            column_config={
                "Room_Revenue": st.column_config.NumberColumn("매출", format="%d원"),
                "RN": st.column_config.NumberColumn("RN", format="%d"),
                "ADR": st.column_config.NumberColumn("ADR", format="%d원"),
            },
            hide_index=True, 
            use_container_width=True
        )
    with c2:
        st.caption("🛏️ 객실타입별 실적")
        rt = data.groupby('Room_Type').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        rt['ADR'] = (rt['Room_Revenue'] / rt['RN']).fillna(0).astype(int)
        # [NEW] 숫자 예쁘게 나오게 포맷팅
        st.dataframe(
            rt.sort_values('Room_Revenue', ascending=False),
            column_config={
                "Room_Revenue": st.column_config.NumberColumn("매출", format="%d원"),
                "RN": st.column_config.NumberColumn("RN", format="%d"),
                "ADR": st.column_config.NumberColumn("ADR", format="%d원"),
            },
            hide_index=True, 
            use_container_width=True
        )

    c3, c4 = st.columns(2)
    with c3:
        st.caption("⏳ 리드타임 분석")
        bins = [-999, 0, 3, 7, 14, 30, 60, 999]
        labels = ['당일', '1-3일', '4-7일', '8-14일', '15-30일', '31-60일', '60일+']
        data['Lead_Group'] = pd.cut(data['Lead_Time'], bins=bins, labels=labels)
        lead = data.groupby('Lead_Group').agg({'RN':'sum'}).reset_index()
        st.bar_chart(lead.set_index('Lead_Group'))
    with c4:
        st.caption("🌍 국적별 비중")
        nat = data.groupby('Nat_Group').agg({'RN':'sum'}).reset_index()
        st.bar_chart(nat.set_index('Nat_Group'))

# ------------------------------------------------------------------------------
# UI 메인
# ------------------------------------------------------------------------------
st.set_page_config(page_title="ARI Dual Core", layout="wide")

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

    st.title("🏛️ 앰버 호텔 경영 리포트 (Dual Core)")

    # --------------------------------------------------------------------------
    # 사이드바
    # --------------------------------------------------------------------------
    with st.sidebar.expander("🛠️ 데이터 초기화", expanded=True):
        if st.button("🗑️ 전체 데이터 삭제 (꼬임 방지)"):
            db_sheet.clear()
            cols = ['Guest_Name', 'CheckIn', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Nat_Group', 'Month_Label', 'Is_Zero_Rate']
            db_sheet.append_row(cols)
            load_data_from_sheet.clear()
            st.success("초기화 완료!")
            time.sleep(1)
            st.rerun()

    st.sidebar.header("📤 데이터 업로드")
    
    # 1. 상세 리스트 업로드 (인사이트용)
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
            df = process_data(f2, "Cancelled")
            if not df.empty:
                db_sheet.append_rows(df.fillna('').astype(str).values.tolist())
                load_data_from_sheet.clear()
                st.success("반영 완료!")
                time.sleep(2)
                st.rerun()

    # 2. OTB 업로드 (버짓 달성용)
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

    # --------------------------------------------------------------------------
    # 데이터 로드
    # --------------------------------------------------------------------------
    raw_data = load_data_from_sheet(db_sheet)
    if len(raw_data) <= 1:
        st.warning("⚠️ 데이터가 없습니다. 파일을 업로드해주세요.")
    else:
        df = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Lead_Time']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0
        
        all_snapshots = sorted(df['Snapshot_Date'].unique(), reverse=True)
        sel_snapshot = st.sidebar.selectbox("기준일(Snapshot)", ["전체 누적"] + all_snapshots)
        
        if sel_snapshot != "전체 누적":
            df = df[df['Snapshot_Date'] <= sel_snapshot]
            
        # [영역 분리]
        df_otb_m = df[df['Segment'] == 'OTB_Month']
        df_otb_t = df[df['Segment'] == 'OTB_Total']
        
        # OTB 제외한 순수 리스트
        df_list = df[~df['Segment'].str.contains('OTB')]
        df_list_bk = df_list[df_list['Status'] == 'Booked']
        df_list_cn = df_list[df_list['Status'] == 'Cancelled']

        curr_month = datetime.now().strftime('%Y-%m')

        # ======================================================================
        # [영역 1] 세일즈 온더북 버짓 달성현황 (OTB 파일 기반)
        # ======================================================================
        st.markdown("### 🎯 세일즈 온더북 버짓 달성현황 (Source: OTB File)")
        
        if not df_otb_m.empty:
            m_rev = df_otb_m['Room_Revenue'].sum()
            m_rn = df_otb_m['RN'].sum()
            m_adr = (m_rev / m_rn) if m_rn > 0 else 0
        else:
            m_rev = 0; m_rn = 0; m_adr = 0
            
        m_budget = budget_df[budget_df['Month'] == curr_month]['Budget'].sum()
        m_achieve = (m_rev / m_budget * 100) if m_budget > 0 else 0

        if not df_otb_t.empty:
            t_rev = df_otb_t['Room_Revenue'].sum()
            t_rn = df_otb_t['RN'].sum()
            t_adr = (t_rev / t_rn) if t_rn > 0 else 0
        else:
            t_rev = 0; t_rn = 0; t_adr = 0
            
        t_budget = budget_df['Budget'].sum()
        t_achieve = (t_rev / t_budget * 100) if t_budget > 0 else 0

        c1, c2 = st.columns(2)
        with c1:
            st.info(f"📅 {curr_month} 당월 달성")
            m1, m2, m3 = st.columns(3)
            m1.metric("달성률", f"{m_achieve:.1f}%", f"{m_rev:,.0f} / {m_budget:,.0f}")
            m2.metric("ADR", f"{m_adr:,.0f}")
            m3.metric("RN", f"{m_rn:,.0f}")
        with c2:
            st.success("🌍 전체 누적 달성")
            k1, k2, k3 = st.columns(3)
            k1.metric("달성률", f"{t_achieve:.1f}%", f"{t_rev:,.0f} / {t_budget:,.0f}")
            k2.metric("ADR", f"{t_adr:,.0f}")
            k3.metric("RN", f"{t_rn:,.0f}")

        st.divider()

        # ======================================================================
        # [영역 2] 예약/취소 상세 인사이트 (리스트 파일 기반)
        # ======================================================================
        st.markdown("### 📊 예약/취소 상세 인사이트 (Source: List File)")
        
        if df_list.empty:
            st.warning("데이터가 없습니다. '상세 리스트' 파일을 업로드해주세요.")
        else:
            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                "🗓️ 월별/주별 추이", 
                "✅ 예약 상세 분석", 
                "❌ 취소 상세 분석", 
                "🆓 0원 예약",
                "📈 합계(RN) 분석"
            ])
            
            with tab1:
                col_a, col_b = st.columns(2)
                with col_a:
                    st.write("**월별 예약 유입 (Booking Date)**")
                    monthly_bk = df_list_bk.groupby('Stay_Month')['RN'].sum().reset_index()
                    st.bar_chart(monthly_bk.set_index('Stay_Month'))
                with col_b:
                    st.write("**주별 유입 추이 (RN)**")
                    cn_neg = df_list_cn.assign(RN = -df_list_cn['RN'])
                    net_df = pd.concat([df_list_bk, cn_neg])
                    weekly = net_df.groupby('Stay_YearWeek')['RN'].sum().reset_index()
                    st.line_chart(weekly.set_index('Stay_YearWeek'))

            with tab2:
                render_full_analysis(df_list_bk, "신규 예약")

            with tab3:
                render_full_analysis(df_list_cn, "취소 내역")

            with tab4:
                zero = df_list_bk[df_list_bk['Is_Zero_Rate']]
                st.write(f"총 {len(zero)}건의 0원/무료 예약")
                st.dataframe(zero[['Guest_Name', 'CheckIn', 'Account', 'Room_Type']], use_container_width=True)

            with tab5:
                # [수정] 합계 매출 삭제 (RN만 유지)
                st.metric("리스트 합산 총 룸나잇 (RN)", f"{df_list_bk['RN'].sum():,.0f} RN")
                st.caption("※ 매출 합계는 제외되었습니다.")

except Exception as e:
    st.error(f"🚨 시스템 오류: {e}")
