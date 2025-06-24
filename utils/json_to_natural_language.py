import pandas as pd

def json_to_natural_language(df):
    userId = df.get("userId")
    startedAt = df.get("startedAt")
    entries = []
    for item in df.get("data"):
        entries.append(f'{item["turn"]}턴에서 {item["riskLevel"]} 위험 자산을 '
                       f'{item["buyCount"]}회 매수하고 {item["sellCount"]}회 매도')
    summary = f'{userId}는 {startedAt}에 시뮬레이션을 시작했으며, ' + ", ".join(entries) + '했습니다.'
    f"{userId}는 "
    return summary

def format_natural_language_summary(df):
    rows = []

    for _, row in df.iterrows():
        user_id = row["userId"]
        started_at = row["startedAt"]

        # 시작 문장
        text = f"우리 아이는 {started_at}부터 다음과 같은 투자 활동을 했습니다:\n"

        for col in df.columns:
            if col in ["userId", "startedAt"]:
                continue  # 이건 문장에 이미 들어갔음

            value = row[col]
            text += f"- {col} 항목의 값은 {value}입니다.\n"

        rows.append({
            "userId": user_id,
            "startedAt": started_at,
            "formatText": text.strip()
        })

    return pd.DataFrame(rows)

