import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime
import plotly.express as px

# 1. 구글 시트 연결 (인증 로직 전체 유지)
def get_gspread_client():
    try:
        creds_info = st.secrets["gcp_service_account"]
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ 인증 오류: {e}")
        return None

# 2. 데이터 처리 엔진 (0원 예약 판별 및 18개 컬럼 무삭제)
def process_data(uploaded_file, status):
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
    
    existing_cols = [c for c in col_map.keys() if c in df_raw.columns]
    df = df_raw[existing_cols].rename(columns=col_map).copy()
    
    # 수치형 변환 및 결측치 0 채우기
    for col in ['Room_Revenue', 'Total_Revenue', 'Rooms', 'Nights']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # [핵심] 0원 예약 판별
    df['Is_Zero_Rate'] = df['Total_Revenue'] <= 0
    df['RN'] = df['Rooms'] * df['Nights']
    
    # [에러 방어 1] ADR 0 나누기 방지
    df['ADR'] = df.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)
    
    for col in ['CheckIn', 'Booking_Date']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    today_dt = datetime.now()
    today_str = today_dt.strftime('%Y-%m-%d')
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

    final_cols = ['Guest_Name', 'CheckIn', 'Booking_Date', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Nat_Group', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Month_Label', 'Is_Zero_Rate']
    return df[final_cols], today_str

# 3. 무삭제 상세 분석 렌더링 (포맷팅 적용)
def render_full_analysis(data, title):
    if data is None or data.empty:
        st.info(f"조회된 {title} 데이터가 없습니다.")
        return
    st.markdown(f"#### 📊 {title} 무삭제 상세 분석 리포트")
    c1, c2 = st.columns(2)
    with c1:
        st.write("**🏢 거래처별 실적 (RN, 매출, ADR)**")
        acc = data.groupby('Account').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        acc['ADR'] = (acc['Room_Revenue'] / acc['RN']).replace([float('inf'), -float('inf')], 0).fillna(0).astype(int)
        st.table(acc.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))
    with c2:
        st.write("**🛏️ 객실 타입별 실적**")
        rt = data.groupby('Room_Type').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        rt['ADR'] = (rt['Room_Revenue'] / rt['RN']).replace([float('inf'), -float('inf')], 0).fillna(0).astype(int)
        st.table(rt.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))

    st.write("**📅 시점 매트릭스 (Segment x Month_Label)**")
    pivot = data.pivot_table(index='Segment', columns='Month_Label', values='RN', aggfunc='sum', fill_value=0)
    st.table(pivot)

# 4. 주기별 트렌드
def render_periodic_trend(data, group_col, label):
    if data is None or data.empty: return
    summary = data.groupby(group_col).agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
    summary['ADR'] = (summary['Room_Revenue'] / summary['RN']).replace([float('inf')], 0).fillna(0).astype(int)
    col1, col2 = st.columns([2, 1])
    with col1:
        st.plotly_chart(px.line(summary, x=group_col, y='Room_Revenue', markers=True, title=f"{label} 트렌드"), use_container_width=True)
    with col2:
        st.table(summary.sort_values(group_col).style.format({'RN':'{:,}', 'Room_Revenue':'{:,}', 'ADR':'{:,}'}))

# --- UI 메인 ---
st.set_page_config(page_title="ARI Executive Dashboard", layout="wide")

try:
    c = get_gspread_client()
    sh = c.open("Amber_Revenue_DB")
    
    # [A방식] Budget 연동
    try:
        budget_sheet = sh.worksheet("Budget")
        budget_raw = budget_sheet.get_all_values()
        budget_df = pd.DataFrame(budget_raw[1:], columns=budget_raw[0])
        budget_df['Budget'] = pd.to_numeric(budget_df['Budget'], errors='coerce').fillna(0)
    except:
        budget_df = pd.DataFrame(columns=['Month', 'Budget'])

    raw = sh.get_worksheet(0).get_all_values()
    if len(raw) > 1:
        db_df = pd.DataFrame(raw[1:], columns=raw[0])
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Lead_Time']:
            db_df[col] = pd.to_numeric(db_df[col], errors='coerce').fillna(0)
        
        db_df['Is_Zero_Rate'] = db_df['Total_Revenue'] <= 0
        all_dates = sorted(db_df['Snapshot_Date'].unique(), reverse=True)
        sel_date = st.sidebar.selectbox("Snapshot 선택", ["전체 누적"] + all_dates)
        
        filtered_df = db_df if sel_date == "전체 누적" else db_df[db_df['Snapshot_Date'] <= sel_date]
        paid_df = filtered_df[filtered_df['Is_Zero_Rate'] == False]
        bk = paid_df[paid_df['Status'] == 'Booked']
        cn = filtered_df[filtered_df['Status'] == 'Cancelled']

        # --- [최상단] 실시간 세일즈온더북 vs 버짓 달성 대시보드 ---
        st.header(f"🏛️ 앰버 호텔 경영 요약 리포트 ({sel_date})")
        
        b_rn, b_rev, b_room = bk['RN'].sum(), bk['Total_Revenue'].sum(), bk['Room_Revenue'].sum()
        total_budget = budget_df['Budget'].sum()
        total_adr = (b_room / b_rn) if b_rn > 0 else 0
        total_achievement = (b_room / total_budget * 100) if total_budget > 0 else 0
        
        st.subheader("🎯 실시간 목표 달성 현황 (Budget vs OTB)")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("🚩 버짓 달성률", f"{total_achievement:.1f} %", delta=f"목표: {total_budget:,.0f}원")
        m2.metric("🏠 총 유료 객실매출", f"{b_room:,.0f} 원")
        m3.metric("💰 총 유료 전체매출", f"{b_rev:,.0f} 원")
        m4.metric("📈 유료 예약 ADR", f"{total_adr:,.0f} 원")

        st.divider()

        # 월별 상세 달성률 테이블
        st.subheader("📅 월별 유료 실적 및 목표 달성 상세")
        m_bk = bk.groupby('Stay_Month').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
        m_total = pd.merge(m_bk, budget_df, left_on='Stay_Month', right_on='Month', how='left').fillna(0)
        m_total['달성률(%)'] = (m_total['Room_Revenue'] / m_total['Budget'] * 100).replace([float('inf')], 0).fillna(0).round(1)

        st.table(m_total[['Stay_Month', 'RN', 'Room_Revenue', 'Budget', '달성률(%)']].style.format({
            'RN':'{:,}', 'Room_Revenue':'{:,}', 'Budget':'{:,}', '달성률(%)':'{}%'
        }))

        # 탭 구성 (무삭제)
        tab_weekly, tab_monthly, tab_det, tab_zero, tab_up = st.tabs([
            "📅 주별 분석", "🗓️ 월별 분석", "📈 무삭제 상세 분석", "🆓 0원 예약 목록", "📤 OTB 업데이트"
        ])
        
        with tab_weekly:
            net_df = pd.concat([bk, cn.assign(RN=-cn['RN'], Room_Revenue=-cn['Room_Revenue'])])
            render_periodic_trend(net_df, 'Stay_YearWeek', '주별')

        with tab_monthly:
            render_periodic_trend(net_df, 'Stay_Month', '월별')

        with tab_det:
            st_bk, st_cn = st.tabs(["✅ 유료 예약 상세", "❌ 유료 취소 상세"])
            with st_bk: render_full_analysis(bk, "유료 예약")
            with st_cn: render_full_analysis(cn, "유료 취소")

        with tab_zero:
            st.subheader("🆓 0원 예약 목록 (체험단/VIP 등)")
            zero_booked = filtered_df[(filtered_df['Is_Zero_Rate'] == True) & (filtered_df['Status'] == 'Booked')]
            st.dataframe(zero_booked[['Guest_Name', 'CheckIn', 'RN', 'Account', 'Room_Type']].style.format({'RN':'{:,}'}), use_container_width=True)

        with tab_up:
            st.subheader("📤 세일즈온더북(OTB) 신규 업로드")
            m = st.radio("업로드 종류", ["신규 예약", "취소 내역"], horizontal=True)
            status = "Booked" if m == "신규 예약" else "Cancelled"
            f = st.file_uploader("엑셀/CSV 파일 선택", type=['csv', 'xlsx'])
            if f and st.button("데이터베이스 업데이트"):
                df_p, _ = process_data(f, status)
                sh.get_worksheet(0).append_rows(df_p.fillna('').astype(str).values.tolist())
                st.success("✅ 업데이트 완료! 대시보드에서 달성률을 확인하세요.")

except Exception as e:
    st.error(f"🚨 시스템 오류: {e}")
