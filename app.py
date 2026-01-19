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
    
    # 합계 행 제거
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
    df['Status'] = status # Booked / Cancelled
    
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
            return f"M+{offset}" if offset > 0 else "M" if offset == 0 else "Past"
        except: return "Unknown"
    df['Month_Label'] = df['CheckIn'].apply(get_month_label)
    
    final_cols = ['Guest_Name', 'CheckIn', 'Booking_Date', 'RN', 'Room_Revenue', 'Total_Revenue', 'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Nat_Group', 'Month_Label', 'Status']
    return df[final_cols], today

# --- 스트림릿 UI 시작 ---
st.set_page_config(page_title="Amber RI Final", layout="wide")
st.title("🏨 Amber Revenue Intelligence (ARI)")

tab_up, tab_rep = st.tabs(["📤 데이터 업로드", "📈 실시간 상세 분석"])

with tab_up:
    m = st.radio("데이터 종류", ["신규 예약", "취소 내역"], horizontal=True)
    curr_status = "Booked" if m == "신규 예약" else "Cancelled"
    f = st.file_uploader(f"{m} 파일 선택", type=['csv', 'xlsx'])
    if f:
        df_p, _ = process_data(f, curr_status)
        st.dataframe(df_p.head(5))
        if st.button(f"{m} 저장"):
            c = get_gspread_client()
            sh = c.open("Amber_Revenue_DB")
            sh.get_worksheet(0).append_rows(df_p.fillna('').astype(str).values.tolist())
            st.success("저장 완료!")

with tab_rep:
    try:
        c = get_gspread_client()
        sh = c.open("Amber_Revenue_DB")
        db_df = pd.DataFrame(sh.get_worksheet(0).get_all_values())
        db_df.columns = db_df.iloc[0]; db_df = db_df[1:]
        for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR']:
            db_df[col] = pd.to_numeric(db_df[col], errors='coerce').fillna(0)
        
        # --- [1] 상단: 전체 합산 현황 (Net) ---
        st.header("🏁 넷 실적 현황 (Total Net Performance)")
        bk = db_df[db_df['Status'] == 'Booked']
        cn = db_df[db_df['Status'] == 'Cancelled']
        n_rn, n_rev = bk['RN'].sum() - cn['RN'].sum(), bk['Room_Revenue'].sum() - cn['Room_Revenue'].sum()
        
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Net RN", f"{n_rn:,.0f}")
        k2.metric("Net Revenue", f"{n_rev:,.0f}")
        k3.metric("Net ADR", f"{n_rev/n_rn if n_rn > 0 else 0:,.0f}")
        k4.metric("취소율", f"{(cn['RN'].sum()/bk['RN'].sum()*100) if bk['RN'].sum()>0 else 0:.1f}%")
        st.divider()

        # --- [2] 하단: 상세 분석 (예약 vs 취소 탭 분리) ---
        st.subheader("🔍 데이터 상세 분석")
        tab_bk, tab_cn = st.tabs(["✅ 신규 예약 상세 (New Bookings)", "❌ 취소 내역 상세 (Cancellations)"])
        
        for t, data, color in zip([tab_bk, tab_cn], [bk, cn], ["#636EFA", "#EF553B"]):
            with t:
                # 거래처 / 룸타입 테이블
                c1, c2 = st.columns(2)
                with c1:
                    st.write("**🏢 거래처별 (RN, 매출, ADR)**")
                    acc = data.groupby('Account').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
                    acc['ADR'] = (acc['Room_Revenue']/acc['RN']).fillna(0).astype(int)
                    st.table(acc.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))
                with c2:
                    st.write("**🛏️ 룸 타입별 (RN, 매출, ADR)**")
                    rt = data.groupby('Room_Type').agg({'RN':'sum','Room_Revenue':'sum'}).reset_index()
                    rt['ADR'] = (rt['Room_Revenue']/rt['RN']).fillna(0).astype(int)
                    st.table(rt.sort_values('Room_Revenue', ascending=False).style.format({'RN':'{:,}','Room_Revenue':'{:,}','ADR':'{:,}'}))
                
                # 국적비 / 추이 차트
                c3, c4 = st.columns(2)
                with c3:
                    st.plotly_chart(px.pie(data, values='Room_Revenue', names='Nat_Group', hole=0.4, title="국적 비중"), use_container_width=True)
                with c4:
                    st.plotly_chart(px.bar(data.groupby('Snapshot_Date')['RN'].sum().reset_index(), x='Snapshot_Date', y='RN', title="일자별 추이", color_discrete_sequence=[color]), use_container_width=True)

    except Exception as e:
        st.info("데이터를 업로드하면 대시보드가 활성화됩니다.")
