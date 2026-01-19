import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime
import plotly.express as px

# 1. 구글 시트 연결 (보안 설정)
def get_gspread_client():
    try:
        creds_info = st.secrets["gcp_service_account"]
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ 인증 오류: {e}")
        return None

# 2. 데이터 처리 엔진 (합계 제외, RN/ADR 계산, 상태 구분)
def process_data(uploaded_file, status):
    if uploaded_file.name.endswith('.csv'):
        df_raw = pd.read_csv(uploaded_file, skiprows=1)
    else:
        df_raw = pd.read_excel(uploaded_file, skiprows=1)
    
    df_raw.columns = df_raw.iloc[0]
    df_raw = df_raw.drop(df_raw.index[0]).reset_index(drop=True)
    
    # 총합계 행 제거 (고객명 기준)
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
    df['Status'] = status # Booked or Cancelled
    
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
st.set_page_config(page_title="ARI Dashboard", layout="wide")
st.title("🏨 Amber Revenue Intelligence (ARI)")

tab_upload, tab_report = st.tabs(["📤 데이터 업로드", "📈 상세 실적 분석"])

with tab_upload:
    mode = st.radio("데이터 종류 선택", ["신규 예약 리스트", "취소 리스트"], horizontal=True)
    current_status = "Booked" if mode == "신규 예약 리스트" else "Cancelled"
    
    file = st.file_uploader(f"{mode} 파일 업로드 (CSV/Excel)", type=['csv', 'xlsx'])
    
    if file:
        df_p, s_date = process_data(file, current_status)
        st.subheader(f"🔍 {s_date} {mode} 미리보기")
        st.dataframe(df_p.head(10))
        
        if st.button(f"{mode} DB 저장"):
            client = get_gspread_client()
            if client:
                sh = client.open("Amber_Revenue_DB")
                worksheet = sh.get_worksheet(0)
                worksheet.append_rows(df_p.fillna('').astype(str).values.tolist())
                st.balloons()
                st.success(f"{mode} 데이터가 구글 시트에 누적되었습니다!")

with tab_report:
    try:
        client = get_gspread_client()
        sh = client.open("Amber_Revenue_DB")
        raw_rows = sh.get_worksheet(0).get_all_values()
        
        if len(raw_rows) <= 1:
            st.info("데이터가 없습니다. 업로드 탭에서 데이터를 먼저 저장하세요.")
        else:
            db_df = pd.DataFrame(raw_rows[1:], columns=raw_rows[0])
            for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR']:
                db_df[col] = pd.to_numeric(db_df[col], errors='coerce').fillna(0)
            
            # --- 1. 상단 넷 픽업 요약 (Net Performance) ---
            st.subheader("🏁 총합계 현황 (Net Pick-up)")
            booked_df = db_df[db_df['Status'] == 'Booked']
            cancel_df = db_df[db_df['Status'] == 'Cancelled']
            
            net_rn = booked_df['RN'].sum() - cancel_df['RN'].sum()
            net_rev = booked_df['Room_Revenue'].sum() - cancel_df['Room_Revenue'].sum()
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Net RN", f"{net_rn:,.0f} RN")
            k2.metric("Net Revenue", f"{net_rev:,.0f} 원")
            k3.metric("Net ADR", f"{net_rev/net_rn if net_rn > 0 else 0:,.0f} 원")
            k4.metric("취소율(RN기준)", f"{(cancel_df['RN'].sum()/booked_df['RN'].sum()*100) if booked_df['RN'].sum()>0 else 0:.1f}%")
            
            st.divider()
            
            # --- 2. 상세 내역 분석 (탭 분리) ---
            st.subheader("🔍 항목별 상세 분석")
            sub_tab1, sub_tab2 = st.tabs(["✅ 신규 예약 (New Bookings)", "❌ 취소 예약 (Cancellations)"])
            
            for sub_tab, data, chart_color in zip([sub_tab1, sub_tab2], [booked_df, cancel_df], ["#636EFA", "#EF553B"]):
                with sub_tab:
                    # 요약 지표 (Account / Room Type)
                    ca, cb = st.columns(2)
                    with ca:
                        st.markdown("**🏢 어카운트별 실적**")
                        sum_acc = data.groupby('Account').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
                        sum_acc['ADR'] = (sum_acc['Room_Revenue']/sum_acc['RN']).fillna(0).astype(int)
                        st.table(sum_acc.sort_values('Room_Revenue', ascending=False).head(10).style.format({'Room_Revenue':'{:,}', 'ADR':'{:,}'}))
                    with cb:
                        st.markdown("**🛏️ 객실 타입별 실적**")
                        sum_rt = data.groupby('Room_Type').agg({'RN':'sum', 'Room_Revenue':'sum'}).reset_index()
                        sum_rt['ADR'] = (sum_rt['Room_Revenue']/sum_rt['RN']).fillna(0).astype(int)
                        st.table(sum_rt.sort_values('Room_Revenue', ascending=False).style.format({'Room_Revenue':'{:,}', 'ADR':'{:,}'}))
                    
                    # 추이 차트
                    st.plotly_chart(px.bar(data.groupby('Snapshot_Date')['RN'].sum().reset_index(), 
                                           x='Snapshot_Date', y='RN', title="일자별 트래픽 추이", 
                                           color_discrete_sequence=[chart_color]), use_container_width=True)

    except Exception as e:
        st.error(f"대시보드 오류: {e}")
