import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime
import plotly.express as px
import numpy as np
import time

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
# 2. [핵심] 지능형 컬럼 매핑 엔진 (띄어쓰기/대소문자 무시)
# ------------------------------------------------------------------------------
def normalize_and_map_columns(df):
    """
    컬럼 이름을 정규화(소문자, 공백제거)하여 핵심 키워드와 매핑합니다.
    """
    col_map = {}
    
    # 매핑 규칙 정의 (우선순위 높음)
    rules = {
        'CheckIn': ['checkin', 'check-in', 'check in', 'arrival', '입실', '일자', 'date'],
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

    # 현재 데이터프레임의 컬럼들을 순회하며 매핑
    for original_col in df.columns:
        # 컬럼명을 소문자로 변환하고 공백/특수문자 제거
        clean_col = str(original_col).lower().replace(" ", "").replace("_", "").replace("-", "")
        
        mapped = False
        for target_col, keywords in rules.items():
            for kw in keywords:
                if kw in clean_col:
                    # 이미 매핑된 타겟 컬럼이 있다면(예: Revenue가 Room_rev와 Total_rev 둘 다에 걸림), 
                    # 더 정확한 매칭을 위해 길이 비교나 우선순위 로직이 필요하지만,
                    # 여기서는 먼저 발견된 것을 우선하되, 'Total' 같은 특정 키워드는 구분함.
                    
                    # 예외: Total Revenue와 Room Revenue 구분
                    if target_col == 'Room_Revenue' and 'total' in clean_col:
                        continue
                    if target_col == 'Total_Revenue' and 'room' in clean_col and 'total' not in clean_col:
                        continue
                        
                    # 예약일자(Booking)와 입실일자(CheckIn) 구분
                    if target_col == 'CheckIn' and ('book' in clean_col or 'res' in clean_col):
                        continue
                        
                    if target_col not in col_map.values(): # 중복 매핑 방지 (단순화)
                        col_map[original_col] = target_col
                        mapped = True
                        break
            if mapped: break
            
    return df.rename(columns=col_map)

def find_valid_header_row(df):
    """실제 헤더가 있는 행을 찾습니다."""
    for i, row in df.iterrows():
        row_str = " ".join(row.astype(str).values).lower()
        # 헤더로 의심되는 키워드가 2개 이상 포함된 줄을 헤더로 인정
        keywords = ['guest', 'name', 'check', 'date', 'room', '고객', '입실', '객실']
        match_count = sum(1 for k in keywords if k in row_str)
        if match_count >= 2:
            df.columns = df.iloc[i]
            return df.iloc[i+1:].reset_index(drop=True)
    return df

# ------------------------------------------------------------------------------
# 3. 데이터 처리 프로세스
# ------------------------------------------------------------------------------
def process_data(uploaded_file, status, sub_segment="General"):
    try:
        is_otb = "Sales on the Book" in uploaded_file.name or "영업 현황" in uploaded_file.name
        
        # 1. 파일 읽기
        if uploaded_file.name.endswith('.csv'):
            df_raw = pd.read_csv(uploaded_file, header=None)
        else:
            df_raw = pd.read_excel(uploaded_file, header=None)

        if is_otb:
            # [영업현황 OTB] - 구조 고정 (지배인님 파일 기준)
            # 보통 4번째 줄부터 데이터
            df_raw = find_valid_header_row(df_raw)
            
            # 소계/합계 제거
            if '일자' in df_raw.columns: 
                df_raw = df_raw[~df_raw['일자'].astype(str).str.contains('소계|Subtotal|합계|Total', na=False)]
            elif df_raw.shape[1] > 0: # 일자 컬럼 못 찾았으면 첫번째 컬럼 기준
                df_raw = df_raw[~df_raw.iloc[:, 0].astype(str).str.contains('소계|Subtotal|합계|Total', na=False)]

            df = pd.DataFrame()
            df['Guest_Name'] = f'OTB_{sub_segment}_DATA'
            
            # 날짜 컬럼 찾기 (없으면 오늘 날짜)
            date_col = next((c for c in df_raw.columns if '일자' in str(c) or 'Date' in str(c)), df_raw.columns[0])
            df['CheckIn'] = pd.to_datetime(df_raw[date_col], errors='coerce')
            
            # [안전] 오른쪽 끝 인덱싱 (합계 섹션)
            try:
                df['RN'] = pd.to_numeric(df_raw.iloc[:, -5], errors='coerce').fillna(0)
                df['Room_Revenue'] = pd.to_numeric(df_raw.iloc[:, -1], errors='coerce').fillna(0) # 맨 끝은 항상 매출
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
            # [예약/취소 리스트] - 컬럼 매핑이 중요
            df_raw = find_valid_header_row(df_raw)
            
            # 소계 제거
            df_raw = df_raw[~df_raw.iloc[:, 0].astype(str).str.contains('합계|Total', na=False)]
            
            # [핵심] 지능형 매핑 적용
            df = normalize_and_map_columns(df_raw).copy()
            
            # [방어 로직] CheckIn 컬럼이 없으면 에러 발생 -> 사용자에게 알림
            if 'CheckIn' not in df.columns:
                st.error(f"🚨 '입실일자(CheckIn)' 컬럼을 찾을 수 없습니다. 파일의 컬럼명: {list(df_raw.columns)}")
                return pd.DataFrame()

            # Booking_Date 없으면 CheckIn으로 대체
            if 'Booking_Date' not in df.columns:
                df['Booking_Date'] = df['CheckIn']
            
            # 필수 컬럼 채우기 (없으면 0 or 빈값)
            required_cols = ['Rooms', 'Nights', 'Room_Revenue', 'Total_Revenue', 'Guest_Name', 'Segment', 'Account', 'Room_Type', 'Nat_Orig']
            for c in required_cols:
                if c not in df.columns: df[c] = 0 if 'Revenue' in c or c in ['Rooms', 'Nights'] else ''

            # 수치 변환
            for col in ['Room_Revenue', 'Total_Revenue', 'Rooms', 'Nights']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            df['RN'] = df['Rooms'] * df['Nights'].replace(0, 1)
            df['ADR'] = df.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)

        # [공통 후처리]
        df['ADR'] = df['ADR'].replace([np.inf, -np.inf], 0).fillna(0)
        df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0 
        df['Snapshot_Date'] = datetime.now().strftime('%Y-%m-%d')
        df['Status'] = status
        
        df['CheckIn_dt'] = pd.to_datetime(df['CheckIn'], errors='coerce')
        df['Booking_dt'] = pd.to_datetime(df['Booking_Date'], errors='coerce')
        # Booking Date 파싱 실패 시 CheckIn으로 대체
        df.loc[df['Booking_dt'].isna(), 'Booking_dt'] = df.loc[df['Booking_dt'].isna(), 'CheckIn_dt']
        
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
                else: return "3.그외"
            except: return "Unknown"
        df['Month_Label'] = df['CheckIn_dt'].apply(get_month_label)

        df['CheckIn'] = df['CheckIn_dt'].dt.strftime('%Y-%m-%d')
        
        cols = ['Guest_Name', 'CheckIn', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Nat_Group', 'Month_Label', 'Is_Zero_Rate']
        
        # 최종적으로 컬럼 갯수 맞추기
        final_df = pd.DataFrame()
        for c in cols:
            final_df[c] = df[c] if c in df.columns else ''
            
        return final_df

    except Exception as e:
        st.error(f"🚨 데이터 처리 중 오류 발생: {e}")
        # 디버깅용: 에러 발생 시 빈 데이터프레임 대신 에러 메시지 포함한 DF 반환 안함 (혼란 방지)
        return pd.DataFrame()

def render_full_analysis(data, title):
    if data is None or data.empty:
        st.info(f"📍 {title} 데이터가 없습니다.")
        return
    st.markdown(f"#### 📊 {title}")
    c1, c2 = st.columns(2)
    with c1:
        acc = data.groupby('Account').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        acc['ADR'] = (acc['Room_Revenue'] / acc['RN']).fillna(0).astype(int)
        st.write(acc.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))
    with c2:
        rt = data.groupby('Room_Type').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        rt['ADR'] = (rt['Room_Revenue'] / rt['RN']).fillna(0).astype(int)
        st.write(rt.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))

# ------------------------------------------------------------------------------
# UI 메인
# ------------------------------------------------------------------------------
st.set_page_config(page_title="ARI Extreme Final", layout="wide")

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
    # 긴급 관리 도구
    # --------------------------------------------------------------------------
    with st.sidebar.expander("🛠️ 데이터 관리 (초기화)", expanded=True):
        if st.button("🗑️ 데이터 초기화 (필수)"):
            db_sheet.clear()
            cols = ['Guest_Name', 'CheckIn', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Nat_Group', 'Month_Label', 'Is_Zero_Rate']
            db_sheet.append_row(cols)
            st.success("초기화 완료! 다시 업로드하세요.")
            time.sleep(1)
            st.rerun()

    # --------------------------------------------------------------------------
    # 업로드 센터
    # --------------------------------------------------------------------------
    st.sidebar.header("📤 데이터 업로드")
    
    with st.sidebar.expander("📝 1. 신규 예약", expanded=False):
        f1 = st.file_uploader("파일 선택", type=['xlsx','csv'], key="f1")
        if f1 and st.button("신규 예약 반영"):
            df = process_data(f1, "Booked")
            if not df.empty:
                db_sheet.append_rows(df.fillna('').astype(str).values.tolist())
                st.success("완료! (자동 새로고침)")
                time.sleep(2)
                st.rerun()

    with st.sidebar.expander("❌ 2. 취소 리스트", expanded=False):
        f2 = st.file_uploader("파일 선택", type=['xlsx','csv'], key="f2")
        if f2 and st.button("취소 내역 반영"):
            df = process_data(f2, "Cancelled")
            if not df.empty:
                db_sheet.append_rows(df.fillna('').astype(str).values.tolist())
                st.success("완료! (자동 새로고침)")
                time.sleep(2)
                st.rerun()

    with st.sidebar.expander("🗓️ 3. 영업현황 (당월)", expanded=True):
        f3 = st.file_uploader("파일 선택", type=['xlsx','csv'], key="f3")
        if f3 and st.button("당월 OTB 반영"):
            df = process_data(f3, "Booked", "Month")
            if not df.empty:
                db_sheet.append_rows(df.fillna('').astype(str).values.tolist())
                st.success("완료! (자동 새로고침)")
                time.sleep(2)
                st.rerun()

    with st.sidebar.expander("🌍 4. 영업현황 (전체)", expanded=True):
        f4 = st.file_uploader("파일 선택", type=['xlsx','csv'], key="f4")
        if f4 and st.button("전체 OTB 반영"):
            df = process_data(f4, "Booked", "Total")
            if not df.empty:
                db_sheet.append_rows(df.fillna('').astype(str).values.tolist())
                st.success("완료! (자동 새로고침)")
                time.sleep(2)
                st.rerun()

    # --------------------------------------------------------------------------
    # 대시보드 로직
    # --------------------------------------------------------------------------
    raw_data = db_sheet.get_all_values()
    
    if len(raw_data) <= 1:
        st.warning("⚠️ 데이터가 없습니다. 파일을 업로드해주세요.")
    else:
        df = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        
        # 수치 변환
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0
        
        all_snapshots = sorted(df['Snapshot_Date'].unique(), reverse=True)
        sel_snapshot = st.sidebar.selectbox("기준일(Snapshot)", ["전체 누적"] + all_snapshots)
        
        if sel_snapshot != "전체 누적":
            df = df[df['Snapshot_Date'] <= sel_snapshot]
            
        paid_df = df[~df['Is_Zero_Rate']].copy()
        curr_month = datetime.now().strftime('%Y-%m')

        st.subheader(f"🎯 버짓 달성 현황 (기준: {sel_snapshot})")
        
        # A. 당월 (OTB_Month 우선)
        otb_m = paid_df[(paid_df['Segment'] == 'OTB_Month') & (paid_df['Status'] == 'Booked')]
        if otb_m.empty: otb_m = paid_df[(paid_df['Status'] == 'Booked') & (paid_df['Stay_Month'] == curr_month)]
            
        m_rev = otb_m['Room_Revenue'].sum()
        m_rn = otb_m['RN'].sum()
        m_budget = budget_df[budget_df['Month'] == curr_month]['Budget'].sum()
        m_achieve = (m_rev / m_budget * 100) if m_budget > 0 else 0

        # B. 전체 (OTB_Total 우선)
        otb_t = paid_df[(paid_df['Segment'] == 'OTB_Total') & (paid_df['Status'] == 'Booked')]
        if otb_t.empty: otb_t = paid_df[paid_df['Status'] == 'Booked']
            
        t_rev = otb_t['Room_Revenue'].sum()
        t_rn = otb_t['RN'].sum()
        t_budget = budget_df['Budget'].sum()
        t_achieve = (t_rev / t_budget * 100) if t_budget > 0 else 0

        c1, c2 = st.columns(2)
        c1.metric(f"{curr_month} 당월 달성률", f"{m_achieve:.1f}%", f"{m_rev:,.0f}원 / {m_budget:,.0f}")
        c2.metric("전체 누적 달성률", f"{t_achieve:.1f}%", f"{t_rev:,.0f}원 / {t_budget:,.0f}")

        st.divider()

        t1, t2, t3, t4 = st.tabs(["🗓️ 월별", "📅 주별", "📈 상세", "🆓 0원"])
        
        with t1:
            m_df = otb_t.groupby('Stay_Month').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
            m_res = pd.merge(m_df, budget_df, left_on='Stay_Month', right_on='Month', how='left').fillna(0)
            m_res['달성률'] = (m_res['Room_Revenue'] / m_res['Budget'] * 100).replace([np.inf, -np.inf], 0).round(1)
            st.dataframe(m_res)
            
        with t2:
            cn = df[(df['Status'] == 'Cancelled') & (~df['Segment'].str.contains('OTB'))]
            cn = cn.assign(Room_Revenue = -cn['Room_Revenue'])
            comb = pd.concat([otb_t, cn])
            w_df = comb.groupby('Stay_YearWeek').agg({'Room_Revenue':'sum'}).reset_index()
            st.plotly_chart(px.line(w_df, x='Stay_YearWeek', y='Room_Revenue', title="주별 순매출"))
            
        with t3:
            bk = paid_df[(~paid_df['Segment'].str.contains('OTB')) & (paid_df['Status']=='Booked')]
            cn = df[(~df['Segment'].str.contains('OTB')) & (df['Status']=='Cancelled')]
            s1, s2 = st.tabs(["예약", "취소"])
            with s1: render_full_analysis(bk, "유료 예약")
            with s2: render_full_analysis(cn, "취소")
            
        with t4:
            z = df[(df['Is_Zero_Rate']) & (~df['Segment'].str.contains('OTB'))]
            st.dataframe(z[['Guest_Name', 'CheckIn', 'Account', 'Room_Type']])

except Exception as e:
    st.error(f"🚨 시스템 오류: {e}")
