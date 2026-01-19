import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime
import plotly.express as px

# 1. 구글 시트 연결
def get_gspread_client():
    try:
        creds_info = st.secrets["gcp_service_account"]
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ 인증 오류: {e}")
        return None

# 2. 데이터 처리 엔진 (지배인님 원본 로직 100% 유지 + 분석 컬럼 보강)
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
    df['ADR'] = df.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)
    df['Lead_Time'] = (df['CheckIn'] - df['Booking_Date']).dt.days.fillna(0).astype(int)
    df['Day_of_Week'] = df['CheckIn'].dt.day_name()
    
    # [추가 분석 컬럼]
    df['Stay_YearWeek'] = df['CheckIn'].dt.strftime('%Y-%U주')
    df['Stay_Month'] = df['CheckIn'].dt.strftime('%Y-%m')
    
    def classify_nat(row):
        name, orig = str(row.get('Guest_Name', '')), str(row.get('Nat_Orig', '')).upper()
        if re.search('[가-힣]', name): return 'KOR'
        if any(x in orig for x in ['CHN', 'HKG', 'TWN', 'MAC']): return 'CHN'
        return 'OTH'
    df['Nat_Group'] = df.apply(classify_nat, axis=1)

    df['CheckIn'] = df['CheckIn'].dt.strftime('%Y-%m-%d')
    df['Booking_Date'] = df['Booking_Date'].dt.strftime('%Y-%m-%d')
    
    def get_month_label(checkin_str):
        try:
            dt = datetime.strptime(checkin_str, '%Y-%m-%d')
            curr = datetime.now()
            offset = (dt.year - curr.year) * 12 + (dt.month - curr.month)
            if offset == 0: return "0.당월(M)"
            elif offset == 1: return "1.익월(M+1)"
            elif offset == 2: return "2.익익월(M+2)"
            elif offset >= 3: return "3.익익익월+(M+3~)"
            else: return "Past"
        except: return "Unknown"
    df['Month_Label'] = df['CheckIn'].apply(get_month_label)
    
    final_cols = ['Guest_Name', 'CheckIn', 'Booking_Date', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Nat_Group', 'Status', 'Stay_Month', 'Stay_YearWeek', 'Lead_Time', 'Day_of_Week', 'Month_Label']
    return df[final_cols], today_str

# 3. 무삭제 분석 렌더링 (지배인님 원본 로직 100% 동일하게 모든 지표 출력)
def render_full_analysis(data, title):
    st.markdown(f"### 📊 {title} 무삭제 상세 분석 리포트")
    
    # 1단: 어카운트 / 룸타입 테이블
    c1, c2 = st.columns(2)
    with c1:
        st.write("🏢 **거래처별 (RN, 매출, ADR)**")
        acc = data.groupby('Account').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        acc['ADR'] = (acc['Room_Revenue']/acc['RN']).fillna(0).astype(int)
        st.table(acc.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))
    with c2:
        st.write("🛏️ **객실 타입별 (RN, 매출, ADR)**")
        rt = data.groupby('Room_Type').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        rt['ADR'] = (rt['Room_Revenue']/rt['RN']).fillna(0).astype(int)
        st.table(rt.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))

    # 2단: 시점별 세그먼트 분석 (Month_Label Matrix)
    st.write("📅 **시점별 세그먼트 분석 (Segment & Month Matrix)**")
    pivot = data.pivot_table(index='Segment', columns='Month_Label', values='RN', aggfunc='sum', fill_value=0)
    st.table(pivot)

    # 3단: 투숙 월별 / 요일별 분석
    c3, c4 = st.columns(2)
    with c3:
        st.write("🗓️ **투숙 월별 실적 (Stay Month)**")
        sm = data.groupby('Stay_Month').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
        sm['ADR'] = (sm['Room_Revenue']/sm['RN']).fillna(0).astype(int)
        st.table(sm.sort_values('Stay_Month').style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))
    with c4:
        st.write("📆 **요일별 입실 분석 (Day of Week)**")
        dow = data.groupby('Day_of_Week').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
        dow['ADR'] = (dow['Room_Revenue']/dow['RN']).fillna(0).astype(int)
        dow_order = {'Monday':0, 'Tuesday':1, 'Wednesday':2, 'Thursday':3, 'Friday':4, 'Saturday':5, 'Sunday':6}
        dow['sort'] = dow['Day_of_Week'].map(dow_order)
        st.table(dow.sort_values('sort').drop('sort', axis=1).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))

    # 4단: 리드타임 / 국적비
    c5, c6 = st.columns(2)
    with c5:
        st.write("⏱️ **세그먼트별 평균 리드타임 (Days)**")
        lt = data.groupby('Segment').agg({'Lead_Time':'mean'}).reset_index()
        st.table(lt.style.format({'Lead_Time':'{:.1f}'}))
    with c6:
        st.plotly_chart(px.pie(data, values='Room_Revenue', names='Nat_Group', hole=0.4, title=f"{title} 국적 비중"), use_container_width=True)

# 4. 주별/월별 전용 트렌드 모듈
def render_periodic_trend(data, group_col, label):
    st.markdown(f"### 📈 {label} 실적 트렌드")
    summary = data.groupby(group_col).agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
    summary['ADR'] = (summary['Room_Revenue'] / summary['RN']).fillna(0).astype(int)
    
    col1, col2 = st.columns([2, 1])
    with col1:
        fig = px.line(summary, x=group_col, y='Room_Revenue', markers=True, title=f"{label} 매출 추이")
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        st.table(summary.sort_values(group_col).style.format({'RN':'{:,}', 'Room_Revenue':'{:,}', 'ADR':'{:,}'}))

# --- UI 메인 ---
st.set_page_config(page_title="ARI Extreme Pro Plus", layout="wide")
st.title("🏨 Amber Revenue Intelligence (ARI)")

# 사이드바: 날짜 필터 (지배인님 요구사항)
st.sidebar.header("🔍 분석 일자 필터")

tab_up, tab_sum, tab_weekly, tab_monthly, tab_det = st.tabs([
    "📤 데이터 업로드", "📋 경영진 요약", "📅 주별 분석", "🗓️ 월별 분석", "📈 무삭제 상세 분석"
])

# 데이터 로딩 및 필터링
try:
    c = get_gspread_client()
    sh = c.open("Amber_Revenue_DB")
    raw = sh.get_worksheet(0).get_all_values()
    
    if len(raw) > 1:
        db_df = pd.DataFrame(raw[1:], columns=raw[0])
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Lead_Time']:
            db_df[col] = pd.to_numeric(db_df[col], errors='coerce').fillna(0)
        
        # 사이드바 날짜 선택기
        all_dates = sorted(db_df['Snapshot_Date'].unique(), reverse=True)
        sel_date = st.sidebar.selectbox("Snapshot 날짜 선택", ["전체 누적 데이터"] + all_dates)
        
        # 필터링 적용
        if sel_date != "전체 누적 데이터":
            filtered_df = db_df[db_df['Snapshot_Date'] == sel_date]
        else:
            filtered_df = db_df

        # 데이터 분리 및 Net 계산
        bk = filtered_df[filtered_df['Status'] == 'Booked']
        cn = filtered_df[filtered_df['Status'] == 'Cancelled']
        cn_neg = cn.copy()
        for col in ['RN', 'Room_Revenue']: cn_neg[col] = -cn_neg[col]
        net_df = pd.concat([bk, cn_neg])

        # --- 탭별 렌더링 ---
        with tab_sum:
            st.header(f"📋 경영진 요약 리포트 ({sel_date})")
            # 픽업 분석 (전체 날짜가 있어야 가능)
            if len(all_dates) >= 2:
                latest, prev = all_dates[0], all_dates[1]
                l_bk = db_df[(db_df['Snapshot_Date']==latest) & (db_df['Status']=='Booked')]
                p_bk = db_df[(db_df['Snapshot_Date']==prev) & (db_df['Status']=='Booked')]
                st.subheader(f"🔄 전일 대비 픽업 ({prev} ➔ {latest})")
                c1, c2, c3 = st.columns(3)
                drn = l_bk['RN'].sum() - p_bk['RN'].sum()
                drev = l_bk['Room_Revenue'].sum() - p_bk['Room_Revenue'].sum()
                c1.metric("예약 픽업 (RN)", f"{drn:,.0f} RN", delta=f"{drn:,.0f}")
                c2.metric("매출 픽업", f"{drev:,.0f} 원", delta=f"{drev:,.0f}")
                c3.metric("최근 취소 발생", f"{len(db_df[(db_df['Snapshot_Date']==latest) & (db_df['Status']=='Cancelled')])} 건", delta_color="inverse")
            st.divider()
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("보유 Net RN", f"{net_df['RN'].sum():,.0f}")
            k2.metric("보유 Net 매출", f"{net_df['Room_Revenue'].sum():,.0f}")
            k3.metric("평균 ADR", f"{net_df['Room_Revenue'].sum()/net_df['RN'].sum() if net_df['RN'].sum()>0 else 0:,.0f}")
            k4.metric("취소율", f"{(cn['RN'].sum()/bk['RN'].sum()*100) if bk['RN'].sum()>0 else 0:.1f}%")

        with tab_weekly:
            render_periodic_trend(net_df, 'Stay_YearWeek', '주별(Weekly)')
        with tab_monthly:
            render_periodic_trend(net_df, 'Stay_Month', '월별(Monthly)')
        with tab_det:
            st_net, st_bk, st_cn = st.tabs(["🏁 전체 합산(Net)", "✅ 신규 예약(Booked)", "❌ 취소 내역(Cancelled)"])
            with st_net: render_full_analysis(net_df, "합산(Net)")
            with st_bk: render_full_analysis(bk, "신규 예약(Booked)")
            with st_cn: render_full_analysis(cn, "취소 내역(Cancelled)")

    else: st.info("데이터를 업로드하세요.")
except Exception as e: st.error(f"오류: {e}")

with tab_up:
    m = st.radio("데이터 종류", ["신규 예약", "취소 내역"], horizontal=True)
    status = "Booked" if m == "신규 예약" else "Cancelled"
    f = st.file_uploader("파일 선택", type=['csv', 'xlsx'])
    if f:
        df_p, _ = process_data(f, status)
        st.dataframe(df_p.head(5))
        if st.button(f"{m} 저장하기"):
            c = get_gspread_client()
            sh = c.open("Amber_Revenue_DB").get_worksheet(0)
            sh.append_rows(df_p.fillna('').astype(str).values.tolist())
            st.balloons(); st.success("저장 완료!")
