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

# 2. 데이터 처리 엔진
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
    
    # [추가] 실제 투숙월 (예: 2026-02) 추출 로직
    df['Stay_Month'] = df['CheckIn'].apply(lambda x: x[:7] if isinstance(x, str) else "Unknown")
    
    final_cols = ['Guest_Name', 'CheckIn', 'Booking_Date', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Nat_Group', 'Month_Label', 'Status', 'Stay_Month']
    return df[final_cols], today

# 3. 무삭제 분석 렌더링 모듈
def render_full_analysis(data, title):
    st.markdown(f"### 📊 {title} 상세 분석")
    
    # 1단: 거래처 / 룸타입 테이블 (생략 없음)
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

    # 2단: 시점별 세그먼트 분석 (당월~익익익월+)
    st.write("📅 **시점별 세그먼트 분석 (당월~익익익월+)**")
    pivot = data.pivot_table(index='Segment', columns='Month_Label', values='RN', aggfunc='sum', fill_value=0)
    try:
        st.table(pivot.style.highlight_max(axis=1))
    except:
        st.table(pivot)

    # 3단: [신규] 투숙 월별 상세 실적 (수익 관리의 핵심)
    st.write("🗓️ **실제 투숙 월별 실적 (Stay Month Analysis)**")
    stay_summary = data.groupby('Stay_Month').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
    stay_summary['ADR'] = (stay_summary['Room_Revenue']/stay_summary['RN']).fillna(0).astype(int)
    st.table(stay_summary.sort_values('Stay_Month').style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))

    # 4단: 국적비 / 추이 차트
    c3, c4 = st.columns(2)
    with c3:
        st.plotly_chart(px.pie(data, values='Room_Revenue', names='Nat_Group', hole=0.4, title=f"{title} 국적 비중"), use_container_width=True)
    with c4:
        color_seq = ["#636EFA"] if "예약" in title or "합산" in title else ["#EF553B"]
        st.plotly_chart(px.bar(data.groupby('Snapshot_Date')['RN'].sum().reset_index(), x='Snapshot_Date', y='RN', title=f"{title} 일자별 유입량", color_discrete_sequence=color_seq), use_container_width=True)

# --- UI 메인 ---
st.set_page_config(page_title="Amber RI - Professional", layout="wide")
st.title("🏨 Amber Revenue Intelligence (ARI)")

tab_up, tab_rep = st.tabs(["📤 데이터 업로드", "📈 실시간 상세 분석"])

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
                st.balloons()
                st.success(f"{m} 저장 완료!")

with tab_rep:
    try:
        c = get_gspread_client()
        sh = c.open("Amber_Revenue_DB")
        raw = sh.get_worksheet(0).get_all_values()
        
        if len(raw) <= 1:
            st.info("데이터를 업로드해 주세요.")
        else:
            db_df = pd.DataFrame(raw[1:], columns=raw[0])
            for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR']:
                db_df[col] = pd.to_numeric(db_df[col], errors='coerce').fillna(0)
            
            bk = db_df[db_df['Status'] == 'Booked']
            cn = db_df[db_df['Status'] == 'Cancelled']
            
            # 넷(Net) 데이터 계산
            net_df = bk.copy()
            cn_neg = cn.copy()
            for col in ['RN', 'Room_Revenue', 'Total_Revenue']:
                cn_neg[col] = -cn_neg[col]
            net_df = pd.concat([net_df, cn_neg])

            t_net, t_bk, t_cn = st.tabs(["🏁 전체 합산(Net)", "✅ 신규 예약(Booked)", "❌ 취소 내역(Cancelled)"])
            
            with t_net:
                n_rn, n_rev = net_df['RN'].sum(), net_df['Room_Revenue'].sum()
                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Net RN", f"{n_rn:,.0f} RN")
                k2.metric("Net Revenue", f"{n_rev:,.0f} 원")
                k3.metric("Net ADR", f"{n_rev/n_rn if n_rn > 0 else 0:,.0f} 원")
                k4.metric("취소율", f"{(cn['RN'].sum()/bk['RN'].sum()*100) if bk['RN'].sum()>0 else 0:.1f}%")
                st.divider()
                render_full_analysis(net_df, "합산(Net)")

            with t_bk:
                render_full_analysis(bk, "신규 예약(Booked)")

            with t_cn:
                render_full_analysis(cn, "취소 내역(Cancelled)")

    except Exception as e:
        st.error(f"오류 발생: {e}")
