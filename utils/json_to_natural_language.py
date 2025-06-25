import pandas as pd

# def json_to_natural_language(df):
#     userId = df.get("userId")
#     startedAt = df.get("startedAt")
#     entries = []
#     for item in df.get("data"):
#         entries.append(f'{item["turn"]}턴에서 {item["riskLevel"]} 위험 자산을 '
#                        f'{item["buyCount"]}회 매수하고 {item["sellCount"]}회 매도')
#     summary = f'{userId}는 {startedAt}에 시뮬레이션을 시작했으며, ' + ", ".join(entries) + '했습니다.'
#     f"{userId}는 "
#     return summary

def format_natural_language_summary(df):
    summaries = []

    for _, row in df.iterrows():
        user_id = row["userId"]

        text = f"""
        우리 아이는 {row['startedAt']}에 시작한 게임에서
        평균 턴 체류시간은 {row['avgStayTime']}초 였고
        평균 태그 턴 체류시간은 {row['tagAvgStayTime']}초 였습니다.
        고위험 자산 평균 구매 비율은 {row['highBuyRatio']}%,
        고위험 자산 평균 판매 비율은 {row['highSellRatio']}% 였습니다.
        중위험 자산 평균 구매 비율은 {row['midBuyRatio']}%,
        중위험 자산 평균 판매 비율은 {row['midSellRatio']}% 였습니다.
        저위험 자산 평균 구매 비율은 {row['lowBuyRatio']}%,
        저위험 자산 평균 판매 비율은 {row['lowSellRatio']}% 였습니다.
        배팅 성공률은 각각 매입시기 예측 성공률 {row['betBuyRatio']}%와 매각시기 예측 성공률 {row['betSellRatio']}% 였습니다.
        평균 현금 보유율은 {row['avgCashRatio']}였습니다.
        """

        summaries.append({
            "userId": user_id,
            "formatText": text.strip()
        })

    df_summary = pd.DataFrame(summaries)
    df_grouped = df_summary.groupby("userId")["formatText"].apply("\n\n".join).reset_index()
    df_grouped.rename(columns={"formatText": "text_user"}, inplace=True)

    summary_columns = [
        "userId", "avgStayTimeMean", "tagAvgStayTimeMean",
        "highBuyRatioMean", "midBuyRatioMean", "lowBuyRatioMean",
        "highSellRatioMean", "midSellRatioMean", "lowSellRatioMean",
        "betBuyRatioMean", "betSellRatioMean", "avgCashRatioMean"
    ]
    df_representative = df[summary_columns].drop_duplicates(subset="userId")

    mean_summaries = []

    for _, row in df_representative.iterrows():
        user_id = row["userId"]
        text = f"""
        우리 아이 나이대의 아이들 평균에 대한 정보입니다.
        평균 턴 체류시간은 {row['avgStayTimeMean']}초, 
        평균 태그 턴 체류시간은 {row['tagAvgStayTimeMean']}초 입니다.
        고위험 자산 평균 구매 비율은 {row['highBuyRatioMean']}%,
        고위험 자산 평균 판매 비율은 {row['highSellRatioMean']}% 입니다.
        중위험 자산 평균 구매 비율은 {row['midBuyRatioMean']}%,
        중위험 자산 평균 판매 비율은 {row['midSellRatioMean']}% 입니다.
        저위험 자산 평균 구매 비율은 {row['lowBuyRatioMean']}%,
        저위험 자산 평균 판매 비율은 {row['lowSellRatioMean']}% 입니다.
        배팅 성공률은 각각
        매입 시기 예측 성공률은 {row['betBuyRatioMean']}%고
        매각 시기 예측 성공률은 {row['betSellRatioMean']}% 입니다.
        평균 현금보유율은 {row['avgCashRatioMean']} 입니다.
        """

        mean_summaries.append({
            "userId": user_id,
            "formatText": text.strip()
        })

    df_mean_summary = pd.DataFrame(mean_summaries)
    df_mean_summary.rename(columns={"formatText": "text_mean"}, inplace=True)

    df_merged = pd.merge(df_grouped, df_mean_summary, on="userId", how="left")

    df_merged["formatText"] = df_merged["text_user"] + "\n\n" + df_merged["text_mean"]
    df_merged.drop(columns=["text_user", "text_mean"], inplace=True)

    return df_merged

