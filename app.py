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

# 2. 데이터 처리 엔진
def process_data(uploaded_file):
    if uploaded_file.name.endswith('.csv'):
        df_raw = pd.read_csv(uploaded_file, skiprows=1)
    else:
        df_raw = pd.read_excel(uploaded_file, skiprows=1)
    
    df_raw.columns = df_raw.iloc[0]
    df_raw = df_raw.drop(df_raw.index[0]).reset_index(drop=True)
    
    # 총합계 행 제거
    df_raw = df_raw[df_raw['고객명'].notna()]
    df_raw = df_raw[~df_raw['고객명'].astype(str).str.contains('합계|Total|소계|합 계', na=False)]
    
    # 컬럼 매핑 (거래처 추가)
    col_map = {
        '고객명': 'Guest_Name', 
        '입실일자': 'CheckIn', 
        '예약일자': 'Booking_Date',
        '객실수': 'Rooms',
        '박수': 'Nights',
        '객실료': 'Room_Revenue',
        '총금액': 'Total_Revenue',
        '시장': 'Segment', 
        '거래처': 'Account', # 어카운트 추가
        '객실타입': 'Room_Type',
        '국적': 'Nat_Orig'
    }
    
    existing_cols = [c for c in col_map.keys() if c in df_raw.columns]
    df = df_raw[existing_cols].rename(columns=col_map).copy()
    
    today = datetime.now().strftime('%Y-%m-%d')
    df['Snapshot_Date'] = today
    
    # 숫자형 변환 및 RN/ADR 계산
    num_cols = ['Room_Revenue', 'Total_Revenue', 'Rooms', 'Nights']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    df['RN'] = df['Rooms'] * df['Nights']
    df['ADR'] = df.apply(lambda x: x['Room_Revenue'] / x['RN'] if x['RN'] > 0 else 0, axis=1)
    
    # 날짜 정리
    for col in ['CheckIn', 'Booking_Date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

    # 국적 판별
    def classify_nat(row):
        name = str(row.get('Guest_Name', ''))
        orig = str(row.get('Nat_Orig', '')).upper()
        if re.search('[가-힣]', name): return 'KOR'
        if any(x in orig for x in ['CHN', 'HKG', 'TWN', 'MAC']): return 'CHN'
        return 'OTH'
    df['Nat_Group'] = df.apply(classify_nat, axis=1)

    # 체크인 월 라벨
    def get_month_label(checkin_str):
        try:
            dt = datetime.strptime(checkin_str, '%Y-%m-%d')
            curr = datetime.now()
            offset = (dt.year - curr.year) * 12 + (dt.month - curr.month)
            return f"M+{offset}" if offset > 0 else "M" if offset == 0 else "Past"
        except: return "Unknown"
    df['Month_Label'] = df['CheckIn'].apply(get_month_label)
    
    # 최종 저장 컬럼 순서 (13개)
    final_cols = [
        'Guest_Name', 'CheckIn', 'Booking_Date', 'RN', 
        'Room_Revenue', 'Total_Revenue', 'ADR', 
        'Segment', 'Account', 'Room_Type', 'Snapshot_Date', 'Nat_Group', 'Month_Label'
    ]
    return df[final_cols], today

# --- 스트림릿 UI ---
st.set_page_config(page_title="Amber Revenue Intelligence", layout="wide")
st.title("📊 Amber Revenue Intelligence (ARI)")

tab1, tab2 = st.tabs(["📤 데이터 업로드", "📈 실시간 실적 분석"])

with tab1:
    st.header("오늘의 PMS 리포트 업로드")
    file = st.file_uploader("파일을 선택하세요 (CSV/Excel)", type=['csv', 'xlsx'])
    
    if file:
        try:
            df_processed, snapshot_date = process_data(file)
            st.success(f"✅ {snapshot_date}자 데이터 분석 완료")
            st.dataframe(df_processed.head(10))

            if st.button("구글 시트(DB)에 누적 저장하기"):
                client = get_gspread_client()
                if client:
                    sh = client.open("Amber_Revenue_DB")
                    worksheet = sh.get_worksheet(0)
                    data_to_save = df_processed.fillna('').astype(str).values.tolist()
                    worksheet.append_rows(data_to_save)
                    st.balloons()
                    st.success("🎉 구글 시트에 데이터가 성공적으로 누적되었습니다!")
        except Exception as e:
            st.error(f"❌ 처리 오류: {e}")

with tab2:
    st.header("📈 실시간 실적 분석")
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
                for col in ['RN', 'Room_Revenue', 'Total_Revenue', 'ADR']:
                    db_df[col] = pd.to_numeric(db_df[col], errors='coerce').fillna(0)
                
                # --- KPI ---
                k1, k2, k3, k4 = st.columns(4)
                total_rn = db_df['RN'].sum()
                total_room_rev = db_df['Room_Revenue'].sum()
                k1.metric("총 룸나잇(RN)", f"{total_rn:,.0f} RN")
                k2.metric("총 객실료", f"{total_room_rev:,.0f} 원")
                k3.metric("총 매출(전체)", f"{db_df['Total_Revenue'].sum():,.0f} 원")
                k4.metric("평균 단가(ADR)", f"{total_room_rev/total_rn if total_rn > 0 else 0:,.0f} 원")
                
                st.divider()

                # --- [신규] 어카운트별 실적 요약 테이블 ---
                st.subheader("🏢 어카운트(거래처)별 실적 요약")
                acc_summary = db_df.groupby('Account').agg({
                    'RN': 'sum',
                    'Room_Revenue': 'sum'
                }).reset_index()
                acc_summary['ADR'] = (acc_summary['Room_Revenue'] / acc_summary['RN']).fillna(0).astype(int)
                acc_summary = acc_summary.sort_values('Room_Revenue', ascending=False).reset_index(drop=True)
                
                # 가독성을 위해 천단위 콤마 추가
                acc_display = acc_summary.copy()
                acc_display['Room_Revenue'] = acc_display['Room_Revenue'].apply(lambda x: f"{x:,.0f}")
                acc_display['ADR'] = acc_display['ADR'].apply(lambda x: f"{x:,.0f}")
                
                st.table(acc_display) # 깔끔한 표로 출력

                st.divider()
                # 기존 차트
                c1, c2 = st.columns(2)
                with c1:
                    st.plotly_chart(px.pie(db_df, values='Room_Revenue', names='Nat_Group', hole=0.4, title="국적별 매출 비중"), use_container_width=True)
                with c2:
                    st.plotly_chart(px.bar(db_df.groupby('Segment')['RN'].sum().reset_index(), x='Segment', y='RN', title="세그먼트별 RN 비중"), use_container_width=True)

    except Exception as e:
        st.error(f"❌ 대시보드 로딩 오류: {e}")
