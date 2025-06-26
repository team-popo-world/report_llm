from dotenv import load_dotenv
from utils.load_db import load_userId
import pandas as pd
import requests

# userId = "956f51a8-d6a0-4a12-a22b-9da3cdffc879"
graph_list = ["avg_stay_time", "buy_ratio", "sell_ratio", "buy_sell_ratio", "bet_ratio", "avg_cash_ratio"]

def update_data(userId, invest_merged_df):
    user_df = pd.DataFrame()
    for graphName in graph_list:
            invest_url = f"http://43.203.175.69:8002/api/invest/{graphName}/week?userId={userId}"
            headers = {"Content-Type": "application/json"}

            response = requests.get(invest_url, headers=headers)

            if response.status_code == 200:
                # print(response.json())  # 응답 데이터
                data = response.json()  # JSON -> Python 객체 (list of dict)
                df = pd.DataFrame(data)  # 리스트를 바로 DataFrame으로 변환
                print(df.head())  # 확인용
                if df.empty:
                    print(f"📭 [EMPTY] userId: {userId}, graph: {graphName}")
                    continue  # 아무것도 안하고 다음으로

                print(f"✅ [RECEIVED] userId: {userId}, graph: {graphName}, rows: {len(df)}")

                # 병합 처리
                if user_df.empty:
                    user_df = df
                else:
                    # 충돌 방지를 위한 공통 컬럼 제거
                    # common_cols = set(user_df.columns).intersection(df.columns)
                    # common_cols -= {"userId", "startedAt"}
                    # user_df = pd.merge(user_df, df, on=["userId", "startedAt"], how="outer")
                    # 병합 키가 둘 다 있는 경우에만 병합 시도
                    merge_keys = ["userId", "startedAt"]
                    if all(key in user_df.columns and key in df.columns for key in merge_keys):
                        user_df = pd.merge(user_df, df, on=merge_keys, how="outer")
                    elif "userId" in user_df.columns and "userId" in df.columns:
                        user_df = pd.merge(user_df, df, on="userId", how="outer")
                    else:
                        print(f"⚠️ 병합 불가: {graphName} 데이터에는 병합 키가 없습니다.")

            else:
                print(f"❌ [ERROR] userId: {userId}, graph: {graphName}")
                print("상태 코드:", response.status_code)

                # 응답이 JSON 형식이면 파싱
                try:
                    error_data = response.json()
                    print("🔍 에러 응답(JSON):", error_data)
                except ValueError:
                    # JSON 형식이 아니면 텍스트 그대로 출력
                    print("🔍 에러 응답(text):", response.text)
    
    if not user_df.empty:
        invest_merged_df = pd.concat([invest_merged_df, user_df], ignore_index=True)
    
    return invest_merged_df
