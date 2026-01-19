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

# 2. 데이터 분석 및 전처리 로직
def process_data(uploaded_file):
    # 확장자에 따라 읽기
    if uploaded_file.name.endswith('.csv'):
        df_raw = pd.read_csv(uploaded_file, skiprows=1)
    else:
        df_raw = pd.read_excel(uploaded_file, skiprows=1)
        
    # 첫 번째 행을 컬럼명으로 설정 및 정리
    df_raw.columns = df_raw.iloc[0]
    df_raw = df_raw.drop(df_raw.index[0]).reset_index(drop=True)
    
    # 컬럼 매핑 (PMS 필드명 -> 시스템 필드명)
    col_map = {
        '고객명': 'Guest_Name', '입실일자': 'CheckIn', '박수': 'RN', 
        '객실타입': 'Room_Type', '객실료': 'Revenue', '시장': 'Segment', '국적': 'Nat_Orig'
    }
    
    existing_cols = [c for c in col_map.keys() if c in df_raw.columns]
    df = df_raw[existing_cols].rename(columns=col_map).copy()
    
    # 데이터 변환 및 스냅샷 날짜 추가
    today = datetime.now().strftime('%Y-%m-%d')
    df['Snapshot_Date'] = today
    
    if 'CheckIn' in df.columns:
        df['CheckIn'] = pd.to_datetime(df['CheckIn'], errors='coerce').dt.strftime('%Y-%m-%d')
    if 'Revenue' in df.columns:
        df['Revenue'] = pd.to_numeric(df['Revenue'], errors='coerce').fillna(0)
    if 'RN' in df.columns:
        df['RN'] = pd.to_numeric(df['RN'], errors='coerce').fillna(0)

    # [지능형 로직] 이름 기반 국적 판별
    def classify_nat(row):
        name = str(row.get('Guest_Name', ''))
        orig = str(row.get('Nat_Orig', '')).upper()
        if re.search('[가-힣]', name): return 'KOR'
        if any(x in orig for x in ['CHN', 'HKG', 'TWN', 'MAC']): return 'CHN'
        return 'OTH'
    
    df['Nat_Group'] = df.apply(classify_nat, axis=1)

    # [지능형 로직] 체크인 월 오프셋 계산 (M, M+1...)
    def get_month_label(checkin_str):
        try:
            dt = datetime.strptime(checkin_str, '%Y-%m-%d')
            curr = datetime.now()
            offset = (dt.year - curr.year) * 12 + (dt.month - curr.month)
            return f"M+{offset}" if offset > 0 else "M" if offset == 0 else "Past"
        except:
            return "Unknown"
            
    df['Month_Label'] = df['CheckIn'].apply(get_month_label)
    
    return df, today

# --- 스트림릿 UI 설정 ---
st.set_page_config(page_title="Amber Revenue Intelligence", layout="wide")
st.title("📊 Amber Revenue Intelligence (ARI)")

tab1, tab2 = st.tabs(["📤 데이터 업로드 및 저장", "📈 실시간 실적 분석"])

# --- TAB 1: 데이터 업로드 섹션 ---
with tab1:
    st.header("오늘의 PMS 리포트 업로드")
    file = st.file_uploader("CSV 또는 Excel 파일을 선택하세요", type=['csv', 'xlsx'])
    
    if file:
        try:
            df_processed, snapshot_date = process_data(file)
            st.subheader(f"🔍 {snapshot_date} 데이터 분석 미리보기")
            st.dataframe(df_processed.head(10))

            if st.button("구글 시트(DB)에 누적 저장하기"):
                client = get_gspread_client()
                if client:
                    sh = client.open("Amber_Revenue_DB")
                    worksheet = sh.get_worksheet(0)
                    
                    # 시트 데이터 전송 준비 (NaN 처리 및 문자열화)
                    data_to_save = df_processed.fillna('').astype(str).values.tolist()
                    worksheet.append_rows(data_to_save)
                    
                    st.balloons()
                    st.success(f"🎉 성공적으로 {len(df_processed)}건의 데이터를 누적했습니다!")
        except Exception as e:
            st.error(f"파일 처리 실패: {e}")

# --- TAB 2: 실시간 분석 대시보드 ---
with tab2:
    st.header("📊 누적 실적 시각화 리포트")
    
    try:
        client = get_gspread_client()
        if client:
            sh = client.open("Amber_Revenue_DB")
            worksheet = sh.get_worksheet(0)
            all_records = worksheet.get_all_records()
            
            if not all_records:
                st.info("데이터베이스에 쌓인 데이터가 없습니다. 먼저 업로드 탭에서 데이터를 저장해 주세요.")
            else:
                db_df = pd.DataFrame(all_records)
                
                # 수치 데이터 변환
                db_df['Revenue'] = pd.to_numeric(db_df['Revenue'], errors='coerce').fillna(0)
                db_df['RN'] = pd.to_numeric(db_df['RN'], errors='coerce').fillna(0)
                
                # --- 상단 주요 지표 (KPI) ---
                kpi1, kpi2, kpi3 = st.columns(3)
                total_rn = db_df['RN'].sum()
                total_rev = db_df['Revenue'].sum()
                avg_adr = total_rev / total_rn if total_rn > 0 else 0
                
                kpi1.metric("누적 총 박수 (RN)", f"{total_rn:,.0f} 박")
                kpi2.metric("누적 총 매출 (REV)", f"{total_rev:,.0f} 원")
                kpi3.metric("평균 판매 단가 (ADR)", f"{avg_adr:,.0f} 원")
                
                st.divider()

                # --- 시각화 차트 ---
                c1, c2 = st.columns(2)
                
                with c1:
                    # 1. 국적별 매출 비중
                    st.subheader("🌐 국적별 매출 비중 (KOR/CHN/OTH)")
                    nat_fig = px.pie(db_df, values='Revenue', names='Nat_Group', hole=0.4,
                                     color_discrete_sequence=px.colors.qualitative.Set3)
                    st.plotly_chart(nat_fig, use_container_width=True)
                
                with c2:
                    # 2. 세그먼트별 RN 비중
                    st.subheader("📊 세그먼트별 점유율 (RN)")
                    seg_df = db_df.groupby('Segment')['RN'].sum().reset_index()
                    seg_fig = px.bar(seg_df, x='Segment', y='RN', color='Segment', text_auto=True)
                    st.plotly_chart(seg_fig, use_container_width=True)

                # 3. 월별(M+n) 예약 추이
                st.subheader("📅 예약 타임라인 (체크인 월별)")
                month_df = db_df.groupby('Month_Label')[['RN', 'Revenue']].sum().reset_index()
                # 정렬용 헬퍼 컬럼
                month_df['sort_idx'] = month_df['Month_Label'].apply(lambda x: int(x.split('+')[1]) if '+' in x else 0)
                month_df = month_df.sort_values('sort_idx')
                
                line_fig = px.line(month_df, x='Month_Label', y='Revenue', markers=True, 
                                   line_shape="spline", title="월별 예상 매출 흐름")
                st.plotly_chart(line_fig, use_container_width=True)

    except Exception as e:
        st.error(f"대시보드 로딩 실패: {e}")
