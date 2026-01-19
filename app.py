import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime
import plotly.express as px

# 1. 구글 시트 연결 (인증 정보 생략 없이 전체 유지)
def get_gspread_client():
    try:
        creds_info = st.secrets["gcp_service_account"]
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ 인증 오류: {e}")
        return None

# 2. 데이터 처리 엔진 (18개 컬럼 무삭제 로직 100% 유지)
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
    
    today_dt = datetime.now()
    today_str = today_dt.strftime('%Y-%m-%d')
    df['Snapshot_Date'] = today_str
    df['Status'] = status
    
    for col in ['Room_Revenue', 'Total_Revenue', 'Rooms', 'Nights']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    for col in ['CheckIn', 'Booking_Date']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
            
    df['RN'] = df['Rooms'] * df['Nights']
    # [에러 방지] 0 나누기 및 Non-finite 값 원천 차단
    df['ADR'] = df.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)
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
    
    final_cols = ['Guest_Name', 'CheckIn', 'Booking_Date', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Nat_Group', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Month_Label']
    return df[final_cols], today_str

# 3. 무삭제 상세 분석 렌더링 함수
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

# 4. 주기별 트렌드 (주별/월별 탭용 호출 함수)
def render_periodic_trend(data, group_col, label):
    if data is None or data.empty:
        st.warning(f"{label} 분석 데이터가 부족합니다.")
        return
    st.markdown(f"### 📈 {label} 매출 트렌드")
    summary = data.groupby(group_col).agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
    summary['ADR'] = (summary['Room_Revenue'] / summary['RN']).replace([float('inf'), -float('inf')], 0).fillna(0).astype(int)
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.plotly_chart(px.line(summary, x=group_col, y='Room_Revenue', markers=True), use_container_width=True)
    with col2:
        st.table(summary.sort_values(group_col).style.format({'RN':'{:,}', 'Room_Revenue':'{:,}', 'ADR':'{:,}'}))

# --- UI 메인 ---
st.set_page_config(page_title="ARI Extreme Pro Dashboard", layout="wide")

try:
    c = get_gspread_client()
    sh = c.open("Amber_Revenue_DB")
    raw = sh.get_worksheet(0).get_all_values()
    
    # [A방식] Budget 시트 연동 로직 (Month, Budget 컬럼 기준)
    try:
        budget_raw = sh.worksheet("Budget").get_all_values()
        budget_df = pd.DataFrame(budget_raw[1:], columns=budget_raw[0])
        budget_df['Budget'] = pd.to_numeric(budget_df['Budget'], errors='coerce').fillna(0)
    except:
        budget_df = pd.DataFrame(columns=['Month', 'Budget'])

    if len(raw) > 1:
        db_df = pd.DataFrame(raw[1:], columns=raw[0])
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Lead_Time']:
            db_df[col] = pd.to_numeric(db_df[col], errors='coerce').fillna(0)
        
        all_dates = sorted(db_df['Snapshot_Date'].unique(), reverse=True)
        sel_date = st.sidebar.selectbox("Snapshot 선택", ["전체 누적"] + all_dates)
        
        filtered_df = db_df if sel_date == "전체 누적" else db_df[db_df['Snapshot_Date'] <= sel_date]
        bk = filtered_df[filtered_df['Status'] == 'Booked']
        cn = filtered_df[filtered_df['Status'] == 'Cancelled']

        # --- [최상단] 경영진 요약 리포트 (8대 지표 대시보드) ---
        st.header(f"🏛️ 앰버 호텔 경영 요약 리포트 ({sel_date})")
        
        # 1. 예약 vs 취소 8대 KPI 카드
        st.subheader("📍 실시간 실적 요약 (Total Summary)")
        k1, k2, k3, k4 = st.columns(4)
        k5, k6, k7, k8 = st.columns(4)
        
        # 예약(Booked) 핵심 지표
        b_rn, b_rev, b_room = bk['RN'].sum(), bk['Total_Revenue'].sum(), bk['Room_Revenue'].sum()
        b_adr = (b_room / b_rn) if b_rn > 0 else 0
        k1.metric("✅ 예약 총 룸나잇", f"{b_rn:,.0f} RN")
        k2.metric("💰 예약 총 매출", f"{b_rev:,.0f} 원")
        k3.metric("🏠 예약 객실 매출", f"{b_room:,.0f} 원")
        k4.metric("📈 예약 ADR", f"{b_adr:,.0f} 원")
        
        # 취소(Cancelled) 핵심 지표
        c_rn, c_rev, c_room = cn['RN'].sum(), cn['Total_Revenue'].sum(), cn['Room_Revenue'].sum()
        c_adr = (c_room / c_rn) if c_rn > 0 else 0
        k5.metric("❌ 취소 총 룸나잇", f"{c_rn:,.0f} RN", delta_color="inverse")
        k6.metric("📉 취소 총 매출", f"{c_rev:,.0f} 원", delta_color="inverse")
        k7.metric("🔻 취소 객실 매출", f"{c_room:,.0f} 원", delta_color="inverse")
        k8.metric("📊 취소 ADR", f"{c_adr:,.0f} 원", delta_color="inverse")
        
        st.divider()
        
        # 2. 월별 예약/취소 상세 분석 테이블 (Budget 포함)
        st.subheader("📅 월별 실적 및 목표 달성률 (Monthly Performance & Budget)")
        
        m_bk = bk.groupby('Stay_Month').agg({'RN':'sum', 'Total_Revenue':'sum', 'Room_Revenue':'sum'}).reset_index()
        m_bk['ADR'] = (m_bk['Room_Revenue'] / m_bk['RN']).replace([float('inf')], 0).fillna(0).astype(int)
        m_bk.columns = ['월', '예약 RN', '예약 총매출', '예약 객실매출', '예약 ADR']
        
        m_cn = cn.groupby('Stay_Month').agg({'RN':'sum', 'Total_Revenue':'sum', 'Room_Revenue':'sum'}).reset_index()
        m_cn['ADR'] = (m_cn['Room_Revenue'] / m_cn['RN']).replace([float('inf')], 0).fillna(0).astype(int)
        m_cn.columns = ['월', '취소 RN', '취소 총매출', '취소 객실매출', '취소 ADR']
        
        # 데이터 병합 및 버짓 계산
        m_total = pd.merge(m_bk, m_cn, on='월', how='outer').fillna(0)
        if not budget_df.empty:
            m_total = pd.merge(m_total, budget_df, left_on='월', right_on='Month', how='left').fillna(0)
            m_total['달성률(%)'] = (m_total['예약 객실매출'] / m_total['Budget'] * 100).replace([float('inf')], 0).fillna(0).round(1)

        st.table(m_total.style.format({
            '예약 RN':'{:,}', '예약 총매출':'{:,}', '예약 객실매출':'{:,}', '예약 ADR':'{:,}',
            '취소 RN':'{:,}', '취소 총매출':'{:,}', '취소 객실매출':'{:,}', '취소 ADR':'{:,}',
            'Budget':'{:,}', '달성률(%)':'{}%'
        }))

        # --- 탭별 무삭제 구성 ---
        tab_sum, tab_weekly, tab_monthly, tab_det, tab_up = st.tabs([
            "📊 점유/채널 요약", "📅 주별 분석", "🗓️ 월별 분석", "📈 무삭제 상세 분석", "📤 데이터 업로드"
        ])
        
        with tab_sum:
            st.subheader("⚡ 실시간 픽업 및 세그먼트 비중")
            if len(all_dates) >= 2:
                latest, prev = all_dates[0], all_dates[1]
                l_bk = db_df[(db_df['Snapshot_Date']==latest) & (db_df['Status']=='Booked')]
                p_bk = db_df[(db_df['Snapshot_Date']==prev) & (db_df['Status']=='Booked')]
                drn, drev = l_bk['RN'].sum() - p_bk['RN'].sum(), l_bk['Room_Revenue'].sum() - p_bk['Room_Revenue'].sum()
                c1, c2 = st.columns(2)
                c1.metric("순증감 (RN)", f"{drn:,.0f} RN", delta=f"{drn:,.0f}")
                c2.metric("매출 증감", f"{drev:,.0f} 원", delta=f"{drev:,.0f}")
            
            st.divider()
            cx, cy = st.columns([2, 1])
            with cx:
                st.plotly_chart(px.bar(m_total, x='월', y=['예약 객실매출', 'Budget'], barmode='group', title="월별 실적 vs 버짓"), use_container_width=True)
            with cy:
                st.plotly_chart(px.pie(bk, values='Room_Revenue', names='Segment', hole=0.4, title="채널별 매출 비중"), use_container_width=True)

        with tab_weekly:
            net_df = pd.concat([bk, cn.assign(RN=-cn['RN'], Room_Revenue=-cn['Room_Revenue'])])
            render_periodic_trend(net_df, 'Stay_YearWeek', '주별(Weekly)')
            
        with tab_monthly:
            render_periodic_trend(net_df, 'Stay_Month', '월별(Monthly)')

        with tab_det:
            st_bk, st_cn = st.tabs(["✅ 예약 상세(Booked)", "❌ 취소 상세(Cancelled)"])
            with st_bk: render_full_analysis(bk, "신규 예약")
            with st_cn: render_full_analysis(cn, "취소 내역")

    with tab_up:
        m = st.radio("종류", ["신규 예약", "취소 내역"], horizontal=True)
        status = "Booked" if m == "신규 예약" else "Cancelled"
        f = st.file_uploader("파일 선택", type=['csv', 'xlsx'])
        if f and st.button("DB 저장하기"):
            df_p, _ = process_data(f, status)
            sh.get_worksheet(0).append_rows(df_p.fillna('').astype(str).values.tolist())
            st.success("✅ 데이터가 성공적으로 저장되었습니다!")

except Exception as e:
    st.error(f"🚨 시스템 오류 발생: {e}")
