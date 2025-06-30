import pandas as pd
import requests

# 🔁 여러 userId 반복 처리
def update_shop_data(user_ids):
    all_rows = []

    for userId in user_ids:
        shop_url = f"http://43.203.175.69:8001/api/dashboard/{userId}"
        try:
            response = requests.get(shop_url, timeout=5)
            response.raise_for_status()
            data = response.json()
            row_df = flatten_shop_data(userId, data)
            all_rows.append(row_df)

        except requests.RequestException as e:
            print(f"❌ 요청 실패: {userId} - {e}")
        except Exception as e:
            print(f"⚠️ 처리 중 오류: {userId} - {e}")

    # 전체 병합
    if all_rows:
        return pd.concat(all_rows, ignore_index=True)
    else:
        return pd.DataFrame()
    
def flatten_shop_data(userId: str, data: dict) -> pd.DataFrame:
    flat = {
        "userId": userId,
        "lastUpdated": data.get("lastUpdated"),
        **data.get("metrics", {})  # metrics는 평탄화
    }

    # 리스트 필드는 그대로 JSON 저장 또는 dict로 유지
    flat["weeklyTrend"] = data.get("weeklyTrend", [])
    flat["categoryData"] = data.get("categoryData", [])
    flat["hourlyData"] = data.get("hourlyData", [])
    flat["popularProducts"] = data.get("popularProducts", [])
    flat["alerts"] = data.get("alerts", [])

    # 필요하면 JSON 문자열로 저장할 수도 있음 (CSV 저장용)
    # flat["weeklyTrendJson"] = json.dumps(flat["weeklyTrend"], ensure_ascii=False)

    return pd.DataFrame([flat])