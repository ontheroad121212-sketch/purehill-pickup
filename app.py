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
# 2. [NEW] 지능형 헤더 탐지 함수 (데이터 밀림 방지)
# ------------------------------------------------------------------------------
def find_valid_header_row(df):
    """
    엑셀 파일에서 실제 헤더가 있는 행을 찾습니다.
    '고객명', 'Name', '일자', 'Date' 등이 포함된 행을 헤더로 인식합니다.
    """
    for i, row in df.iterrows():
        # 행의 값들을 문자열로 합쳐서 키워드 검색
        row_str = " ".join(row.astype(str).values).lower()
        if any(x in row_str for x in ['고객명', 'guest', 'name', '일자', 'date', 'checkin']):
            # 이 행을 헤더로 설정하고 그 아래 데이터만 리턴
            df.columns = df.iloc[i]
            return df.iloc[i+1:].reset_index(drop=True)
    return df  # 못 찾으면 원본 반환

# ------------------------------------------------------------------------------
# 3. 데이터 처리 엔진
# ------------------------------------------------------------------------------
def process_data(uploaded_file, status, sub_segment="General"):
    try:
        is_otb = "Sales on the Book" in uploaded_file.name or "영업 현황" in uploaded_file.name
        
        # 파일 읽기 (헤더 없이 일단 다 읽음)
        if uploaded_file.name.endswith('.csv'):
            df_raw = pd.read_csv(uploaded_file, header=None)
        else:
            df_raw = pd.read_excel(uploaded_file, header=None)

        if is_otb:
            # [영업현황 OTB]
            # OTB는 보통 상단에 제목이 많으므로 4번째 줄 근처 탐색 or 키워드 탐색
            df_raw = find_valid_header_row(df_raw)
            
            # 소계/합계 제거
            if '일자' in df_raw.columns:
                df_raw = df_raw[df_raw['일자'].notna()]
                df_raw = df_raw[~df_raw['일자'].astype(str).str.contains('소계|Subtotal|합계|Total', na=False)]
            
            df = pd.DataFrame()
            df['Guest_Name'] = f'OTB_{sub_segment}_DATA'
            
            # 날짜 컬럼 찾기 (일자, Date)
            date_col = next((c for c in df_raw.columns if '일자' in str(c) or 'Date' in str(c)), None)
            if date_col:
                df['CheckIn'] = pd.to_datetime(df_raw[date_col], errors='coerce')
            else:
                df['CheckIn'] = datetime.now() # 비상시

            # [안전] 오른쪽 끝에서부터 인덱싱 (합계 섹션 타격)
            # 보통 맨 오른쪽=매출, 그 왼쪽=ADR 등
            try:
                df['RN'] = pd.to_numeric(df_raw.iloc[:, -5], errors='coerce').fillna(0) # 뒤에서 5번째
                df['Room_Revenue'] = pd.to_numeric(df_raw.iloc[:, -1], errors='coerce').fillna(0) # 맨 뒤
                df['ADR'] = pd.to_numeric(df_raw.iloc[:, -3], errors='coerce').fillna(0) # 뒤에서 3번째
            except:
                # 인덱싱 실패 시 0 처리
                df['RN'] = 0
                df['Room_Revenue'] = 0
                df['ADR'] = 0

            df['Total_Revenue'] = df['Room_Revenue']
            df['Booking_Date'] = df['CheckIn']
            df['Segment'] = f'OTB_{sub_segment}'
            df['Account'] = 'OTB_Summary'
            df['Room_Type'] = 'Run of House'
            df['Nat_Orig'] = 'KOR'
            
        else:
            # [상세 리스트 - 예약/취소]
            df_raw = find_valid_header_row(df_raw)
            
            # 소계 제거
            col_name_check = df_raw.columns[0]
            df_raw = df_raw[~df_raw[col_name_check].astype(str).str.contains('합계|Total', na=False)]
            
            # [핵심 수정] 매핑 사전 확장 (한글/영어/변형 모두 대응)
            col_map = {}
            for col in df_raw.columns:
                c = str(col).strip()
                if c in ['고객명', 'Guest Name', 'Guest_Name', '투숙객']: col_map[col] = 'Guest_Name'
                elif c in ['입실일자', 'CheckIn', 'Arrival']: col_map[col] = 'CheckIn'
                elif c in ['예약일자', 'Booking Date', 'Create Date']: col_map[col] = 'Booking_Date'
                elif c in ['객실수', 'Rooms', 'Qty', 'RmWs']: col_map[col] = 'Rooms'
                elif c in ['박수', 'Nights', 'Los']: col_map[col] = 'Nights'
                elif c in ['객실료', 'Room Revenue', 'Room_Revenue', 'Revenue']: col_map[col] = 'Room_Revenue'
                elif c in ['총금액', 'Total Revenue', 'Amount']: col_map[col] = 'Total_Revenue'
                elif c in ['시장', 'Segment', 'Mkt Seg']: col_map[col] = 'Segment'
                elif c in ['거래처', 'Account', 'Source']: col_map[col] = 'Account'
                elif c in ['객실타입', 'Room Type', 'Room']: col_map[col] = 'Room_Type'
                elif c in ['국적', 'Nation', 'Country']: col_map[col] = 'Nat_Orig'

            df = df_raw.rename(columns=col_map).copy()
            
            # [중요] Booking_Date가 없거나 이상하면 CheckIn으로 대체 (1, 2 같은 숫자 방지)
            if 'Booking_Date' not in df.columns:
                df['Booking_Date'] = df['CheckIn']
            
            # 수치 변환 (문자 섞임 방지)
            for col in ['Room_Revenue', 'Total_Revenue', 'Rooms', 'Nights']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                else:
                    df[col] = 0
            
            # RN, ADR 계산
            df['RN'] = df['Rooms'] * df['Nights'].replace(0, 1) # 박수가 0이면 1로 처리
            df['ADR'] = df.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)

        # 공통 후처리
        df['ADR'] = df['ADR'].replace([np.inf, -np.inf], 0).fillna(0)
        df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0 
        df['Snapshot_Date'] = datetime.now().strftime('%Y-%m-%d')
        df['Status'] = status
        
        df['CheckIn_dt'] = pd.to_datetime(df['CheckIn'], errors='coerce')
        # Booking_Date가 숫자로 들어와서 엉망이면 CheckIn으로 덮어씀
        df['Booking_dt'] = pd.to_datetime(df['Booking_Date'], errors='coerce')
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
                elif offset == 2: return "2.익익월(M+2)"
                elif offset >= 3: return "3.익익익월+(M+3~)"
                else: return "Past"
            except: return "Unknown"
        df['Month_Label'] = df['CheckIn_dt'].apply(get_month_label)

        df['CheckIn'] = df['CheckIn_dt'].dt.strftime('%Y-%m-%d')
        
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
        acc['ADR'] = (acc['Room_Revenue'] / acc['RN']).replace([np.inf, -np.inf], 0).fillna(0).astype(int)
        st.table(acc.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))
    with c2:
        st.write("**🛏️ 객실타입별**")
        rt = data.groupby('Room_Type').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        rt['ADR'] = (rt['Room_Revenue'] / rt['RN']).replace([np.inf, -np.inf], 0).fillna(0).astype(int)
        st.table(rt.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))

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
    # [긴급] 데이터 초기화 및 확인
    # --------------------------------------------------------------------------
    with st.sidebar.expander("🛠️ 데이터베이스 관리 (필수)", expanded=True):
        if st.button("🗑️ 데이터 초기화 (꼬인 데이터 삭제)"):
            db_sheet.clear()
            cols = ['Guest_Name', 'CheckIn', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Nat_Group', 'Month_Label', 'Is_Zero_Rate']
            db_sheet.append_row(cols)
            st.success("초기화 완료! 파일을 다시 업로드해주세요.")
            time.sleep(1)
            st.rerun()
            
        if st.button("🔍 현재 데이터 확인"):
            raw = db_sheet.get_all_values()
            st.write(f"현재 행 개수: {len(raw)}개")
            if len(raw) > 1:
                st.dataframe(pd.DataFrame(raw[1:], columns=raw[0]).head(5))

    # --------------------------------------------------------------------------
    # 1. 사이드바 - 업로드
    # --------------------------------------------------------------------------
    st.sidebar.header("📤 데이터 업로드")
    
    with st.sidebar.expander("📝 1. 신규 예약", expanded=False):
        f1 = st.file_uploader("신규 파일", type=['xlsx','csv'], key="f1")
        if f1 and st.button("신규 반영"):
            df_new = process_data(f1, "Booked")
            if not df_new.empty:
                db_sheet.append_rows(df_new.fillna('').astype(str).values.tolist())
                st.success("저장 완료!")
                time.sleep(2)
                st.rerun()

    with st.sidebar.expander("❌ 2. 취소 리스트", expanded=False):
        f2 = st.file_uploader("취소 파일", type=['xlsx','csv'], key="f2")
        if f2 and st.button("취소 반영"):
            df_cn = process_data(f2, "Cancelled")
            if not df_cn.empty:
                db_sheet.append_rows(df_cn.fillna('').astype(str).values.tolist())
                st.success("저장 완료!")
                time.sleep(2)
                st.rerun()

    with st.sidebar.expander("🗓️ 3. 영업현황 (당월)", expanded=True):
        f3 = st.file_uploader("당월 OTB", type=['xlsx','csv'], key="f3")
        if f3 and st.button("당월 반영"):
            df_m = process_data(f3, "Booked", "Month")
            if not df_m.empty:
                db_sheet.append_rows(df_m.fillna('').astype(str).values.tolist())
                st.success("저장 완료!")
                time.sleep(2)
                st.rerun()

    with st.sidebar.expander("🌍 4. 영업현황 (전체)", expanded=True):
        f4 = st.file_uploader("전체 OTB", type=['xlsx','csv'], key="f4")
        if f4 and st.button("전체 반영"):
            df_t = process_data(f4, "Booked", "Total")
            if not df_t.empty:
                db_sheet.append_rows(df_t.fillna('').astype(str).values.tolist())
                st.success("저장 완료!")
                time.sleep(2)
                st.rerun()

    # --------------------------------------------------------------------------
    # 2. 데이터 로드 및 전처리
    # --------------------------------------------------------------------------
    raw_data = db_sheet.get_all_values()
    
    if len(raw_data) <= 1:
        st.warning("⚠️ 데이터가 없습니다. '데이터 초기화' 후 파일을 다시 올려주세요.")
    else:
        df = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        
        # 수치형 변환
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        # Is_Zero_Rate 재계산
        df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0
        
        all_snapshots = sorted(df['Snapshot_Date'].unique(), reverse=True)
        sel_snapshot = st.sidebar.selectbox("Snapshot 선택", ["전체 누적"] + all_snapshots)
        
        if sel_snapshot != "전체 누적":
            df = df[df['Snapshot_Date'] <= sel_snapshot]
            
        paid_df = df[~df['Is_Zero_Rate']].copy()
        curr_month = datetime.now().strftime('%Y-%m')

        # ----------------------------------------------------------------------
        # 3. 대시보드
        # ----------------------------------------------------------------------
        st.subheader(f"🎯 실시간 버짓 달성 현황 (기준: {sel_snapshot})")
        
        otb_m = paid_df[(paid_df['Segment'] == 'OTB_Month') & (paid_df['Status'] == 'Booked')]
        if otb_m.empty: otb_m = paid_df[(paid_df['Status'] == 'Booked') & (paid_df['Stay_Month'] == curr_month)]
            
        m_rev = otb_m['Room_Revenue'].sum()
        m_rn = otb_m['RN'].sum()
        m_adr = (m_rev / m_rn) if m_rn > 0 else 0
        m_budget = budget_df[budget_df['Month'] == curr_month]['Budget'].sum()
        m_achieve = (m_rev / m_budget * 100) if m_budget > 0 else 0

        otb_t = paid_df[(paid_df['Segment'] == 'OTB_Total') & (paid_df['Status'] == 'Booked')]
        if otb_t.empty: otb_t = paid_df[paid_df['Status'] == 'Booked']
            
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
        # 4. 분석 탭
        # ----------------------------------------------------------------------
        t1, t2, t3, t4 = st.tabs(["🗓️ 월별 분석", "📅 주별 추이", "📈 상세 리포트", "🆓 0원 예약"])
        
        with t1:
            monthly = otb_t.groupby('Stay_Month').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
            monthly = pd.merge(monthly, budget_df, left_on='Stay_Month', right_on='Month', how='left').fillna(0)
            monthly['달성률(%)'] = (monthly['Room_Revenue'] / monthly['Budget'] * 100).replace([np.inf, -np.inf], 0).round(1)
            st.table(monthly.style.format({'RN':'{:,}', 'Room_Revenue':'{:,}', 'Budget':'{:,}', '달성률(%)':'{}%'}))
            
        with t2:
            cn_df = df[(df['Status'] == 'Cancelled') & (df['Segment'].str.contains('OTB') == False)]
            cn_df = cn_df.assign(RN = -cn_df['RN'], Room_Revenue = -cn_df['Room_Revenue'])
            combined = pd.concat([otb_t, cn_df])
            weekly = combined.groupby('Stay_YearWeek').agg({'Room_Revenue':'sum'}).reset_index()
            st.plotly_chart(px.line(weekly, x='Stay_YearWeek', y='Room_Revenue', title="주별 순매출 추이"), use_container_width=True)
            
        with t3:
            pure_bk = paid_df[(paid_df['Segment'].str.contains('OTB') == False) & (paid_df['Status'] == 'Booked')]
            pure_cn = df[(df['Segment'].str.contains('OTB') == False) & (df['Status'] == 'Cancelled')]
            sub_t1, sub_t2 = st.tabs(["예약 상세", "취소 상세"])
            with sub_t1: render_full_analysis(pure_bk, "유료 예약")
            with sub_t2: render_full_analysis(pure_cn, "취소 내역")
            
        with t4:
            zero_df = df[(df['Is_Zero_Rate'] == True) & (df['Segment'].str.contains('OTB') == False)]
            st.dataframe(zero_df[['Guest_Name', 'CheckIn', 'Account', 'Room_Type']])

except Exception as e:
    st.error(f"🚨 시스템 오류: {e}")
