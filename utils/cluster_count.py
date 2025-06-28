import pandas as pd
import requests

def update_cluster_data(user_list):
    all_user_df = pd.DataFrame()

    for user_id in user_list:
        print(f"🚀 처리 중: {user_id}")
        url = f"http://43.203.175.69:8002/api/invest/invest_style/all?userId={user_id}"
        headers = {"Content-Type": "application/json"}

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            if not data:
                print(f"📭 [EMPTY] userId: {user_id}")
                continue

            # 단일 딕셔너리일 경우 리스트로 변환
            if isinstance(data, dict):
                data = [data]

            df = pd.DataFrame(data)
            df["userId"] = user_id  # userId 컬럼 추가

            all_user_df = pd.concat([all_user_df, df], ignore_index=True)
            print(f"✅ [RECEIVED] userId: {user_id}, rows: {len(df)}")

        else:
            print(f"❌ [ERROR] userId: {user_id}, 상태 코드: {response.status_code}")
            try:
                print("🔍 에러 응답:", response.json())
            except ValueError:
                print("🔍 에러 응답:", response.text)

    # 집계 처리
    if all_user_df.empty:
        print("📭 전체 사용자에 대해 유효한 데이터가 없습니다.")
        return pd.DataFrame()

    # pivot으로 변환
    pivot_df = all_user_df.pivot_table(
        index="userId",
        columns="cluster_num",
        values="count",
        fill_value=0
    )
    pivot_df.columns = [f"cluster_{int(col)+1}" for col in pivot_df.columns]
    pivot_df = pivot_df.reset_index()

    # 누락된 cluster_1 ~ cluster_5 보정
    for i in range(1, 6):
        col = f"cluster_{i}"
        if col not in pivot_df.columns:
            pivot_df[col] = 0  # 누락된 경우 0으로 채움

    # 순서 정렬
    pivot_df = pivot_df[["userId"] + [f"cluster_{i}" for i in range(1, 6)]]

    return pivot_df