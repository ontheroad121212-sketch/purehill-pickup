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

# 2. 데이터 처리 엔진 (기존 모든 로직 유지)
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
    
    today = datetime.now().strftime('%Y-%m-%d')
    df['Snapshot_Date'] = today
    df['Status'] = status
    
    for col in ['Room_Revenue', 'Total_Revenue', 'Rooms', 'Nights']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    df['RN'] = df['Rooms'] * df['Nights']
    df['ADR'] = df.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)
    
    for col in ['CheckIn', 'Booking_Date']:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

    def classify_nat(row):
        name, orig = str(row.get('Guest_Name', '')), str(row.get('Nat_Orig', '')).upper()
        if re.search('[가-힣]', name): return 'KOR'
        if any(x in orig for x in ['CHN', 'HKG', 'TWN', 'MAC']): return 'CHN'
        return 'OTH'
    df['Nat_Group'] = df.apply(classify_nat, axis=1)

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
    df['Stay_Month'] = df['CheckIn'].apply(lambda x: x[:7] if isinstance(x, str) else "Unknown")
    
    final_cols = ['Guest_Name', 'CheckIn', 'Booking_Date', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Nat_Group', 'Month_Label', 'Status', 'Stay_Month']
    return df[final_cols], today

# 3. 상세 분석 렌더링 모듈 (모든 탭에서 호출 - 절대 생략 금지)
def render_full_analysis(data, title):
    st.markdown(f"### 📊 {title} 무삭제 상세 분석")
    
    # 1단: 거래처 / 룸타입 테이블
    c1, c2 = st.columns(2)
    with c1:
        st.write("🏢 **거래처별 실적 (Account)**")
        acc = data.groupby('Account').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        acc['ADR'] = (acc['Room_Revenue']/acc['RN']).fillna(0).astype(int)
        st.table(acc.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))
    
    with c2:
        st.write("🛏️ **객실 타입별 실적 (Room Type)**")
        rt = data.groupby('Room_Type').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
        rt['ADR'] = (rt['Room_Revenue']/rt['RN']).fillna(0).astype(int)
        st.table(rt.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))

    # 2단: 시점별 세그먼트 분석 매트릭스
    st.write("📅 **시점별 세그먼트 분석 (당월~익익익월+)**")
    pivot = data.pivot_table(index='Segment', columns='Month_Label', values='RN', aggfunc='sum', fill_value=0)
    st.table(pivot)

    # 3단: 투숙 월별 실적
    st.write("🗓️ **실제 투숙 월별 실적 (Stay Month)**")
    stay = data.groupby('Stay_Month').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
    stay['ADR'] = (stay['Room_Revenue']/stay['RN']).fillna(0).astype(int)
    st.table(stay.sort_values('Stay_Month').style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))

    # 4단: 국적비 / 추이 차트
    c3, c4 = st.columns(2)
    with c3:
        st.plotly_chart(px.pie(data, values='Room_Revenue', names='Nat_Group', hole=0.4, title=f"{title} 국적 비중"), use_container_width=True)
    with c4:
        # 상태에 따라 색상 결정
        color_seq = ["#636EFA"] if "취소" not in title else ["#EF553B"]
        st.plotly_chart(px.bar(data.groupby('Snapshot_Date')['RN'].sum().reset_index(), x='Snapshot_Date', y='RN', title=f"{title} 일자별 유입량", color_discrete_sequence=color_seq), use_container_width=True)

# --- 메인 UI ---
st.set_page_config(page_title="ARI Final Professional", layout="wide")
st.title("🏨 Amber Revenue Intelligence (ARI)")

tab_up, tab_sum, tab_det = st.tabs(["📤 데이터 업로드", "📋 요약 리포트 (Summary)", "📈 상세 분석 (Details)"])

with tab_up:
    m = st.radio("데이터 종류", ["신규 예약", "취소 내역"], horizontal=True)
    curr_status = "Booked" if m == "신규 예약" else "Cancelled"
    f = st.file_uploader(f"{m} 파일 선택", type=['csv', 'xlsx'])
    if f:
        df_p, _ = process_data(f, curr_status)
        st.dataframe(df_p.head(5))
        if st.button(f"{m} 저장하기"):
            c = get_gspread_client()
            if c:
                sh = c.open("Amber_Revenue_DB")
                sh.get_worksheet(0).append_rows(df_p.fillna('').astype(str).values.tolist())
                st.balloons(); st.success(f"{m} 저장 완료!")

try:
    c = get_gspread_client()
    sh = c.open("Amber_Revenue_DB")
    raw = sh.get_worksheet(0).get_all_values()
    
    if len(raw) > 1:
        db_df = pd.DataFrame(raw[1:], columns=raw[0])
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR']:
            db_df[col] = pd.to_numeric(db_df[col], errors='coerce').fillna(0)
        
        bk = db_df[db_df['Status'] == 'Booked']
        cn = db_df[db_df['Status'] == 'Cancelled']
        
        # Net 계산용 데이터프레임
        cn_neg = cn.copy()
        for col in ['RN', 'Room_Revenue', 'Total_Revenue']: cn_neg[col] = -cn_neg[col]
        net_df = pd.concat([bk, cn_neg])

        with tab_sum:
            st.header("📋 One-Page 핵심 요약")
            
            # 1. 전일 대비 픽업 분석
            dates = sorted(db_df['Snapshot_Date'].unique(), reverse=True)
            if len(dates) >= 2:
                latest, prev = dates[0], dates[1]
                l_bk = db_df[(db_df['Snapshot_Date']==latest) & (db_df['Status']=='Booked')]
                p_bk = db_df[(db_df['Snapshot_Date']==prev) & (db_df['Status']=='Booked')]
                
                st.subheader(f"🔄 전일 대비 픽업 ({prev} ➔ {latest})")
                c1, c2, c3 = st.columns(3)
                diff_rn = l_bk['RN'].sum() - p_bk['RN'].sum()
                diff_rev = l_bk['Room_Revenue'].sum() - p_bk['Room_Revenue'].sum()
                c1.metric("예약 픽업 (RN)", f"{diff_rn:,.0f} RN", delta=f"{diff_rn:,.0f}")
                c2.metric("매출 픽업", f"{diff_rev:,.0f} 원", delta=f"{diff_rev:,.0f}")
                c3.metric("오늘 발생 취소", f"{len(db_df[(db_df['Snapshot_Date']==latest) & (db_df['Status']=='Cancelled')])} 건", delta_color="inverse")
            
            st.divider()
            
            # 2. 총 예약 지표 (Gross Booking)
            st.subheader("💎 총 예약 보유 현황 (Gross Booking)")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("총 예약 RN", f"{bk['RN'].sum():,.0f} RN")
            k2.metric("총 객실 매출", f"{bk['Room_Revenue'].sum():,.0f} 원")
            k3.metric("보유 ADR", f"{bk['Room_Revenue'].sum()/bk['RN'].sum() if bk['RN'].sum()>0 else 0:,.0f} 원")
            k4.metric("넷 실적(Net RN)", f"{net_df['RN'].sum():,.0f} RN")
            
            st.divider()
            # 간단 그래프
            g1, g2 = st.columns(2)
            with g1: st.plotly_chart(px.pie(bk, values='Room_Revenue', names='Nat_Group', hole=0.4, title="예약 국적비"), use_container_width=True)
            with g2: st.plotly_chart(px.bar(bk.groupby('Stay_Month')['RN'].sum().reset_index(), x='Stay_Month', y='RN', title="월별 점유 현황"), use_container_width=True)

        with tab_det:
            # 상세 분석 탭 (여기에서 절대 생략 안 함)
            st_net, st_bk, st_cn = st.tabs(["🏁 전체 합산(Net)", "✅ 신규 예약(Booked)", "❌ 취소 내역(Cancelled)"])
            with st_net: render_full_analysis(net_df, "전체 합산(Net)")
            with st_bk: render_full_analysis(bk, "신규 예약(Booked)")
            with st_cn: render_full_analysis(cn, "취소 내역(Cancelled)")
    else:
        st.info("데이터를 업로드해 주세요.")

except Exception as e:
    st.error(f"오류 발생: {e}")
