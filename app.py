import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime

# 1. 구글 시트 연결 (기존과 동일)
def get_gspread_client():
    creds_info = st.secrets["gcp_service_account"]
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_info, scopes=scope)
    return gspread.authorize(creds)

# 2. 데이터 분석 로직 (Excel 지원 추가)
def process_data(uploaded_file):
    # 확장자에 따라 읽는 방식 변경
    if uploaded_file.name.endswith('.csv'):
        df_raw = pd.read_csv(uploaded_file, skiprows=1)
    else:
        # 엑셀 파일(.xlsx) 읽기
        df_raw = pd.read_excel(uploaded_file, skiprows=1)
        
    df_raw.columns = df_raw.iloc[0]
    df_raw = df_raw.drop(df_raw.index[0]).reset_index(drop=True)
    
    # 컬럼 매핑 (기존과 동일)
    col_map = {
        '고객명': 'Guest_Name', '입실일자': 'CheckIn', '박수': 'RN', 
        '객실타입': 'Room_Type', '객실료': 'Revenue', '시장': 'Segment', '국적': 'Nat_Orig'
    }
    # 실제 파일에 있는 컬럼만 필터링 (에러 방지)
    existing_cols = [c for c in col_map.keys() if c in df_raw.columns]
    df = df_raw[existing_cols].rename(columns=col_map).copy()
    
    today = datetime.now().strftime('%Y-%m-%d')
    df['Snapshot_Date'] = today
    
    # 데이터 타입 변환
    if 'CheckIn' in df.columns:
        df['CheckIn'] = pd.to_datetime(df['CheckIn'], errors='coerce').dt.strftime('%Y-%m-%d')
    if 'Revenue' in df.columns:
        df['Revenue'] = pd.to_numeric(df['Revenue'], errors='coerce').fillna(0)
    if 'RN' in df.columns:
        df['RN'] = pd.to_numeric(df['RN'], errors='coerce').fillna(0)

    # 국적 판별
    def classify_nat(row):
        name = str(row.get('Guest_Name', ''))
        orig = str(row.get('Nat_Orig', '')).upper()
        if re.search('[가-힣]', name): return 'KOR'
        if any(x in orig for x in ['CHN', 'HKG', 'TWN', 'MAC']): return 'CHN'
        return 'OTH'
    
    df['Nat_Group'] = df.apply(classify_nat, axis=1)
    
    return df, today

# --- UI 부분 ---
st.set_page_config(page_title="Amber Revenue Intelligence", layout="wide")
st.title("📊 Amber Revenue Intelligence (ARI)")

tab1, tab2 = st.tabs(["📤 데이터 업로드", "📈 실적 분석 리포트"])

with tab1:
    # type에 'xlsx' 추가
    file = st.file_uploader("PMS 예약 목록 파일을 업로드하세요 (CSV 또는 엑셀)", type=['csv', 'xlsx'])
    
    if file:
        try:
            df_processed, snapshot_date = process_data(file)
            st.write(f"### {snapshot_date} 분석 미리보기")
            st.dataframe(df_processed.head())

            if st.button("구글 시트에 실시간 누적하기"):
                client = get_gspread_client()
                sh = client.open("Amber_Revenue_DB")
                worksheet = sh.get_worksheet(0)
                
                # 시트에 데이터 전송 (NaN 처리를 위해 문자열 변환)
                data_to_append = df_processed.fillna('').astype(str).values.tolist()
                worksheet.append_rows(data_to_append)
                
                st.balloons()
                st.success("🎉 데이터가 구글 시트에 성공적으로 저장되었습니다!")
        except Exception as e:
            st.error(f"파일 처리 중 오류가 발생했습니다: {e}")
