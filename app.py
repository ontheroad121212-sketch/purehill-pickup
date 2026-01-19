import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime

# 1. 구글 시트 연결 (보안 강화 버전)
def get_gspread_client():
    # Streamlit Secrets에서 모든 인증 정보를 한 번에 가져옵니다.
    creds_info = st.secrets["gcp_service_account"]
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_info, scopes=scope)
    return gspread.authorize(creds)

# 2. 데이터 분석 로직 (변화 없음)
def process_data(uploaded_file):
    df_raw = pd.read_csv(uploaded_file, skiprows=1)
    df_raw.columns = df_raw.iloc[0]
    df_raw = df_raw.drop(df_raw.index[0]).reset_index(drop=True)
    
    col_map = {
        '고객명': 'Guest_Name', '입실일자': 'CheckIn', '박수': 'RN', 
        '객실타입': 'Room_Type', '객실료': 'Revenue', '시장': 'Segment', '국적': 'Nat_Orig'
    }
    df = df_raw.rename(columns=col_map)[list(col_map.values())].copy()
    
    today = datetime.now().strftime('%Y-%m-%d')
    df['Snapshot_Date'] = today
    df['CheckIn'] = pd.to_datetime(df['CheckIn'], errors='coerce')
    df['Revenue'] = pd.to_numeric(df['Revenue'], errors='coerce').fillna(0)
    df['RN'] = pd.to_numeric(df['RN'], errors='coerce').fillna(0)

    def classify_nat(row):
        name = str(row['Guest_Name'])
        orig = str(row['Nat_Orig']).upper()
        if re.search('[가-힣]', name): return 'KOR'
        if any(x in orig for x in ['CHN', 'HKG', 'TWN', 'MAC']): return 'CHN'
        return 'OTH'
    df['Nat_Group'] = df.apply(classify_nat, axis=1)

    def get_month_label(dt):
        if pd.isna(dt): return "Unknown"
        curr = datetime.now()
        offset = (dt.year - curr.year) * 12 + (dt.month - curr.month)
        return f"M+{offset}" if offset > 0 else "M" if offset == 0 else "Past"
    df['Month_Label'] = df['CheckIn'].apply(get_month_label)
    
    return df, today

# --- UI 부분 ---
st.set_page_config(page_title="Amber Revenue Intelligence", layout="wide")
st.title("📊 Amber Revenue Intelligence (ARI)")

tab1, tab2 = st.tabs(["📤 데이터 업로드", "📈 실적 분석 리포트"])

with tab1:
    file = st.file_uploader("PMS '전체 고객 목록' CSV 파일을 업로드하세요", type=['csv'])
    if file:
        df_processed, snapshot_date = process_data(file)
        st.dataframe(df_processed.head())

        if st.button("구글 시트에 실시간 누적하기"):
            try:
                client = get_gspread_client()
                sh = client.open("Amber_Revenue_DB") # 구글 시트 이름 확인!
                worksheet = sh.get_worksheet(0)
                data_to_append = df_processed.astype(str).values.tolist()
                worksheet.append_rows(data_to_append)
                st.balloons()
                st.success("🎉 데이터가 구글 시트에 안전하게 누적되었습니다!")
            except Exception as e:
                st.error(f"오류: {e}")

with tab2:
    st.header("실시간 분석 차트")
    # 나중에 데이터가 쌓이면 차트 그리는 코드를 여기에 추가하면 됩니다.
