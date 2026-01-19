import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime
import plotly.express as px

# 1. 구글 시트 연결 (Streamlit Secrets 보안 적용)
def get_gspread_client():
    try:
        creds_info = st.secrets["gcp_service_account"]
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"구글 인증 설정 오류: {e}")
        return None

# 2. 데이터 분석 및 전처리 로직 (RN 계산 추가)
def process_data(uploaded_file):
    if uploaded_file.name.endswith('.csv'):
        df_raw = pd.read_csv(uploaded_file, skiprows=1)
    else:
        df_raw = pd.read_excel(uploaded_file, skiprows=1)
        
    df_raw.columns = df_raw.iloc[0]
    df_raw = df_raw.drop(df_raw.index[0]).reset_index(drop=True)
    
    # 컬럼 매핑 (객실수와 박수 각각 추출)
    col_map = {
        '고객명': 'Guest_Name', 
        '입실일자': 'CheckIn', 
        '예약일자': 'Booking_Date',
        '객실수': 'Rooms',      # 추가
        '박수': 'Nights',      # 추가
        '객실타입': 'Room_Type', 
        '객실료': 'Revenue', 
        '시장': 'Segment', 
        '국적': 'Nat_Orig'
    }
    
    existing_cols = [c for c in col_map.keys() if c in df_raw.columns]
    df = df_raw[existing_cols].rename(columns=col_map).copy()
    
    # 오늘 날짜 (Snapshot)
    today = datetime.now().strftime('%Y-%m-%d')
    df['Snapshot_Date'] = today
    
    # 1단계: 숫자 데이터 변환
    for col in ['Revenue', 'Rooms', 'Nights']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    # ★ 2단계: 룸나잇(RN) 계산 (객실수 * 박수)
    df['RN'] = df['Rooms'] * df['Nights']
    
    # 3단계: 날짜 데이터 변환
    for col in ['CheckIn', 'Booking_Date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

    # [지능형 로직] 국적 판별
    def classify_nat(row):
        name = str(row.get('Guest_Name', ''))
        orig = str(row.get('Nat_Orig', '')).upper()
        if re.search('[가-힣]', name): return 'KOR'
        if any(x in orig for x in ['CHN', 'HKG', 'TWN', 'MAC']): return 'CHN'
        return 'OTH'
    df['Nat_Group'] = df.apply(classify_nat, axis=1)

    # [지능형 로직] 체크인 월 오프셋 (M, M+1...)
    def get_month_label(checkin_str):
        try:
            dt = datetime.strptime(checkin_str, '%Y-%m-%d')
            curr = datetime.now()
            offset = (dt.year - curr.year) * 12 + (dt.month - curr.month)
            return f"M+{offset}" if offset > 0 else "M" if offset == 0 else "Past"
        except: return "Unknown"
    df['Month_Label'] = df['CheckIn'].apply(get_month_label)
    
    # 최종 DB 저장용 컬럼 순서 정리
    final_cols = ['Guest_Name', 'CheckIn', 'Booking_Date', 'RN', 'Revenue', 'Segment', 'Room_Type', 'Snapshot_Date', 'Nat_Group', 'Month_Label']
    return df[final_cols], today

# --- 스트림릿 UI ---
st.set_page_config(page_title="Amber Revenue Intelligence", layout="wide")
st.title("📊 Amber Revenue Intelligence (ARI)")

tab1, tab2 = st.tabs(["📤 데이터 업로드 및 저장", "📈 실시간 실적 분석"])

with tab1:
    st.header("오늘의 PMS 리포트 업로드")
    file = st.file_uploader("파일을 선택하세요 (CSV/Excel)", type=['csv', 'xlsx'])
    
    if file:
        try:
            df_processed, snapshot_date = process_data(file)
            st.success(f"✅ {snapshot_date}자 분석 완료! (RN 계산됨)")
            st.dataframe(df_processed.head(10))

            if st.button("구글 시트(DB)에 누적 저장하기"):
                client = get_gspread_client()
                if client:
                    sh = client.open("Amber_Revenue_DB")
                    worksheet = sh.get_worksheet(0)
                    data_to_save = df_processed.fillna('').astype(str).values.tolist()
                    worksheet.append_rows(data_to_save)
                    st.balloons()
                    st.success("🎉 데이터가 성공적으로 누적되었습니다!")
        except Exception as e:
            st.error(f"파일 처리 실패: {e}")

with tab2:
    st.header("📊 실시간 실적 분석")
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
                db_df['Revenue'] = pd.to_numeric(db_df['Revenue'], errors='coerce').fillna(0)
                db_df['RN'] = pd.to_numeric(db_df['RN'], errors='coerce').fillna(0)
                
                # KPI 계산
                total_rn = db_df['RN'].sum()
                total_rev = db_df['Revenue'].sum()
                avg_adr = total_rev / total_rn if total_rn > 0 else 0
                
                k1, k2, k3 = st.columns(3)
                k1.metric("총 룸나잇 (RN)", f"{total_rn:,.0f} RN")
                k2.metric("총 매출액 (REV)", f"{total_rev:,.0f} 원")
                k3.metric("평균 단가 (ADR)", f"{avg_adr:,.0f} 원")
                
                st.divider()
                # (이후 차트 코드는 동일하게 작동합니다)
                c1, c2 = st.columns(2)
                with c1:
                    st.plotly_chart(px.pie(db_df, values='Revenue', names='Nat_Group', hole=0.4, title="국적별 매출 비중"), use_container_width=True)
                with c2:
                    st.plotly_chart(px.bar(db_df.groupby('Segment')['RN'].sum().reset_index(), x='Segment', y='RN', title="세그먼트별 RN"), use_container_width=True)

    except Exception as e:
        st.error(f"대시보드 로딩 실패: {e}")
