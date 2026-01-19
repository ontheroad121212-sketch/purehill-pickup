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
        st.error(f"❌ 구글 시트 인증 오류: {e}")
        return None

# 2. 데이터 처리 엔진 (Status 추가)
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
    df['Status'] = status # 'Booked' 또는 'Cancelled'
    
    # 숫자 변환 및 계산
    for col in ['Room_Revenue', 'Total_Revenue', 'Rooms', 'Nights']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    df['RN'] = df['Rooms'] * df['Nights']
    df['ADR'] = df.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)
    
    # 날짜 정리
    for col in ['CheckIn', 'Booking_Date']:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

    # 국적 판별
    def classify_nat(row):
        name = str(row.get('Guest_Name', ''))
        orig = str(row.get('Nat_Orig', '')).upper()
        if re.search('[가-힣]', name): return 'KOR'
        if any(x in orig for x in ['CHN', 'HKG', 'TWN', 'MAC']): return 'CHN'
        return 'OTH'
    df['Nat_Group'] = df.apply(classify_nat, axis=1)

    # 투숙 월 라벨
    def get_month_label(checkin_str):
        try:
            dt = datetime.strptime(checkin_str, '%Y-%m-%d')
            curr = datetime.now()
            offset = (dt.year - curr.year) * 12 + (dt.month - curr.month)
            return f"M+{offset}" if offset > 0 else "M" if offset == 0 else "Past"
        except: return "Unknown"
    df['Month_Label'] = df['CheckIn'].apply(get_month_label)
    
    # 최종 저장 컬럼 (14개)
    final_cols = [
        'Guest_Name', 'CheckIn', 'Booking_Date', 'RN', 'Room_Revenue', 'Total_Revenue', 
        'ADR', 'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Nat_Group', 'Month_Label', 'Status'
    ]
    return df[final_cols], today

# --- 스트림릿 UI ---
st.set_page_config(page_title="Amber Revenue Intelligence", layout="wide")
st.title("📊 Amber Revenue Intelligence (ARI)")

tab1, tab2 = st.tabs(["📤 데이터 업로드", "📈 실시간 실적 분석"])

with tab1:
    st.header("오늘의 리포트 업로드")
    
    # [핵심] 데이터 성격 선택
    data_type = st.radio("업로드 데이터 종류를 선택하세요", ["신규 예약 리스트", "취소 예약 리스트"])
    status = "Booked" if data_type == "신규 예약 리스트" else "Cancelled"
    
    file = st.file_uploader(f"{data_type} 파일을 업로드하세요", type=['csv', 'xlsx'])
    
    if file:
        try:
            df_processed, snapshot_date = process_data(file, status)
            st.success(f"✅ {snapshot_date}자 {data_type} 분석 완료")
            st.dataframe(df_processed.head(10))

            if st.button(f"{data_type} DB 누적 저장"):
                client = get_gspread_client()
                if client:
                    sh = client.open("Amber_Revenue_DB")
                    worksheet = sh.get_worksheet(0)
                    data_to_save = df_processed.fillna('').astype(str).values.tolist()
                    worksheet.append_rows(data_to_save)
                    st.balloons()
                    st.success(f"🎉 {data_type} 데이터가 성공적으로 저장되었습니다!")
        except Exception as e:
            st.error(f"❌ 오류: {e}")

with tab2:
    st.header("📈 실시간 실적 및 넷 픽업 분석")
    try:
        client = get_gspread_client()
        if client:
            sh = client.open("Amber_Revenue_DB")
            worksheet = sh.get_worksheet(0)
            raw_data = worksheet.get_all_values()
            
            if len(raw_data) <= 1:
                st.info("데이터가 없습니다.")
            else:
                db_df = pd.DataFrame(raw_data[1:], columns=raw_data[0])
                for col in ['RN', 'Room_Revenue', 'Total_Revenue']:
                    db_df[col] = pd.to_numeric(db_df[col], errors='coerce').fillna(0)
                
                # --- [핵심] 넷 픽업 계산 로직 ---
                booked = db_df[db_df['Status'] == 'Booked']
                cancelled = db_df[db_df['Status'] == 'Cancelled']
                
                net_rn = booked['RN'].sum() - cancelled['RN'].sum()
                net_rev = booked['Room_Revenue'].sum() - cancelled['Room_Revenue'].sum()
                
                # --- KPI ---
                k1, k2, k3, k4 = st.columns(4)
                k1.metric("넷 픽업 (Net RN)", f"{net_rn:,.0f} RN", help="신규 예약 - 취소")
                k2.metric("넷 매출 (Net REV)", f"{net_rev:,.0f} 원")
                k3.metric("총 취소 RN", f"{cancelled['RN'].sum():,.0f} RN", delta=f"-{cancelled['RN'].sum()}", delta_color="inverse")
                k4.metric("넷 ADR", f"{net_rev/net_rn if net_rn > 0 else 0:,.0f} 원")
                
                st.divider()

                # --- 픽업 트렌드 차트 ---
                st.subheader("🗓️ 일자별 넷 픽업 추이 (Net Pick-up Trend)")
                # 스냅샷별로 예약과 취소 합산
                trend_booked = booked.groupby('Snapshot_Date')['RN'].sum().reset_index()
                trend_cancelled = cancelled.groupby('Snapshot_Date')['RN'].sum().reset_index()
                
                trend_df = pd.merge(trend_booked, trend_cancelled, on='Snapshot_Date', how='outer', suffixes=('_New', '_Cancel')).fillna(0)
                trend_df['Net_Pickup'] = trend_df['RN_New'] - trend_df['RN_Cancel']
                
                fig_trend = px.bar(trend_df, x='Snapshot_Date', y=['RN_New', 'RN_Cancel'], 
                                   title="신규 예약 vs 취소 (일자별 비교)", barmode='group')
                st.plotly_chart(fig_trend, use_container_width=True)

                st.divider()
                # 기존 거래처/룸타입 표... (생략 가능하나 유지됨)
                st.subheader("🏢 거래처별 넷 픽업 실적")
                acc_booked = booked.groupby('Account').agg({'RN':'sum', 'Room_Revenue':'sum'})
                acc_cancel = cancelled.groupby('Account').agg({'RN':'sum', 'Room_Revenue':'sum'})
                acc_net = (acc_booked - acc_cancel).fillna(acc_booked).fillna(-acc_cancel).fillna(0).reset_index()
                st.table(acc_net.sort_values('Room_Revenue', ascending=False).head(10))

    except Exception as e:
        st.error(f"❌ 로딩 오류: {e}")
