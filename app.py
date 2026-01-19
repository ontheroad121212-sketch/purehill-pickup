import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime

# 1. 구글 시트 연결 설정
def get_gspread_client():
    # 서비스 계정 키 파일 경로 (파일명이 다르면 수정하세요)
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('google_key.json', scopes=scope)
    return gspread.authorize(creds)

# 2. 분석 핵심 로직 (Brain)
def process_data(uploaded_file):
    # 데이터 로드
    df_raw = pd.read_csv(uploaded_file, skiprows=1)
    df_raw.columns = df_raw.iloc[0]
    df_raw = df_raw.drop(df_raw.index[0]).reset_index(drop=True)
    
    # 필수 컬럼 정리
    col_map = {
        '고객명': 'Guest_Name', '입실일자': 'CheckIn', '박수': 'RN', 
        '객실타입': 'Room_Type', '객실료': 'Revenue', '시장': 'Segment', '국적': 'Nat_Orig'
    }
    df = df_raw.rename(columns=col_map)[list(col_map.values())].copy()
    
    # 오늘 날짜 (Snapshot) 및 데이터 변환
    today = datetime.now().strftime('%Y-%m-%d')
    df['Snapshot_Date'] = today
    df['CheckIn'] = pd.to_datetime(df['CheckIn'], errors='coerce')
    df['Revenue'] = pd.to_numeric(df['Revenue'], errors='coerce').fillna(0)
    df['RN'] = pd.to_numeric(df['RN'], errors='coerce').fillna(0)

    # [지능형 판별 1] 국적 그룹화
    def classify_nat(row):
        name = str(row['Guest_Name'])
        orig = str(row['Nat_Orig']).upper()
        if re.search('[가-힣]', name): return 'KOR'
        if any(x in orig for x in ['CHN', 'HKG', 'TWN', 'MAC']): return 'CHN'
        return 'OTH'
    df['Nat_Group'] = df.apply(classify_nat, axis=1)

    # [지능형 판별 2] 체크인 월 오프셋 (M, M+1...)
    def get_month_label(dt):
        if pd.isna(dt): return "Unknown"
        curr = datetime.now()
        offset = (dt.year - curr.year) * 12 + (dt.month - curr.month)
        return f"M+{offset}" if offset > 0 else "M" if offset == 0 else "Past"
    df['Month_Label'] = df['CheckIn'].apply(get_month_label)
    
    return df, today

# --- 스트림릿 UI 시작 ---
st.set_page_config(page_title="Amber Revenue Intelligence", layout="wide")
st.title("📊 Amber Revenue Intelligence (ARI)")

tab1, tab2 = st.tabs(["📤 데이터 업로드", "📈 실적 분석 리포트"])

with tab1:
    st.header("오늘의 예약 데이터 업로드")
    file = st.file_uploader("PMS '전체 고객 목록' CSV 파일을 업로드하세요", type=['csv'])
    
    if file:
        df_processed, snapshot_date = process_data(file)
        st.success(f"✅ {snapshot_date}자 데이터 분석 완료!")
        st.dataframe(df_processed.head())

        if st.button("구글 시트에 실시간 누적하기"):
            try:
                client = get_gspread_client()
                # 구글 시트 이름 확인 필수!
                sh = client.open("Amber_Revenue_DB")
                worksheet = sh.get_worksheet(0) # 첫 번째 시트
                
                # 데이터 전송 (헤더 제외하고 데이터만)
                data_to_append = df_processed.astype(str).values.tolist()
                worksheet.append_rows(data_to_append)
                
                st.balloons()
                st.success("🎉 구글 시트에 성공적으로 누적되었습니다!")
            except Exception as e:
                st.error(f"오류 발생: {e} (구글 시트 이름을 확인하고 봇을 초대했는지 체크하세요!)")

with tab2:
    st.header("누적 데이터 시각화")
    st.info("여기에 누적된 데이터를 바탕으로 한 실시간 그래프가 표시됩니다 (데이터가 쌓이면 자동 활성화).")
    # 나중에 여기에 실시간 차트 코드를 추가할 예정입니다!
