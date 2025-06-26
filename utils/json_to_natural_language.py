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
        # 컬럼 존재 여부 확인 + 값이 NaN인지 확인 후 기본값 0 처리
        avg_stay_time = row["avgStayTime"] if "avgStayTime" in row and pd.notna(row["avgStayTime"]) else 0
        tag_avg_stay_time = row["tagAvgStayTime"] if "tagAvgStayTime" in row and pd.notna(row["tagAvgStayTime"]) else 0
        high_buy_ratio = row["highBuyRatio"] if "highBuyRatio" in row and pd.notna(row["highBuyRatio"]) else 0
        high_sell_ratio = row["highSellRatio"] if "highSellRatio" in row and pd.notna(row["highSellRatio"]) else 0
        mid_buy_ratio = row["midBuyRatio"] if "midBuyRatio" in row and pd.notna(row["midBuyRatio"]) else 0
        mid_sell_ratio = row["midSellRatio"] if "midSellRatio" in row and pd.notna(row["midSellRatio"]) else 0
        low_buy_ratio = row["lowBuyRatio"] if "lowBuyRatio" in row and pd.notna(row["lowBuyRatio"]) else 0
        low_sell_ratio = row["lowSellRatio"] if "lowSellRatio" in row and pd.notna(row["lowSellRatio"]) else 0
        bet_buy_ratio = row["betBuyRatio"] if "betBuyRatio" in row and pd.notna(row["betBuyRatio"]) else 0
        bet_sell_ratio = row["betSellRatio"] if "betSellRatio" in row and pd.notna(row["betSellRatio"]) else 0
        avg_cash_ratio = row["avgCashRatio"] if "avgCashRatio" in row and pd.notna(row["avgCashRatio"]) else 0

        text = f"""
        우리 아이는 {row['startedAt']}에 시작한 게임에서
        평균 턴 체류시간은 {avg_stay_time}초 였고
        평균 태그 턴 체류시간은 {tag_avg_stay_time}초 였습니다.
        고위험 자산 평균 구매 비율은 {high_buy_ratio}%,
        고위험 자산 평균 판매 비율은 {high_sell_ratio}% 였습니다.
        중위험 자산 평균 구매 비율은 {mid_buy_ratio}%,
        중위험 자산 평균 판매 비율은 {mid_sell_ratio}% 였습니다.
        저위험 자산 평균 구매 비율은 {low_buy_ratio}%,
        저위험 자산 평균 판매 비율은 {low_sell_ratio}% 였습니다.
        배팅 성공률은 각각 매입시기 예측 성공률 {bet_buy_ratio}%와 매각시기 예측 성공률 {bet_sell_ratio}% 였습니다.
        평균 현금 보유율은 {avg_cash_ratio}였습니다.
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
    summary_columns = [col for col in summary_columns if col in df.columns]
    df_representative = df[summary_columns].drop_duplicates(subset="userId")

    mean_summaries = []

    for _, row in df_representative.iterrows():
        user_id = row["userId"]
        avg_stay_time_m = row["avgStayTimeMean"] if "avgStayTimeMean" in row and pd.notna(row["avgStayTimeMean"]) else 0
        tag_avg_stay_time_m = row["tagAvgStayTimeMean"] if "tagAvgStayTimeMean" in row and pd.notna(row["tagAvgStayTimeMean"]) else 0
        high_buy_ratio_m = row["highBuyRatioMean"] if "highBuyRatioMean" in row and pd.notna(row["highBuyRatioMean"]) else 0
        high_sell_ratio_m = row["highSellRatioMean"] if "highSellRatioMean" in row and pd.notna(row["highSellRatioMean"]) else 0
        mid_buy_ratio_m = row["midBuyRatioMean"] if "midBuyRatioMean" in row and pd.notna(row["midBuyRatioMean"]) else 0
        mid_sell_ratio_m = row["midSellRatioMean"] if "midSellRatioMean" in row and pd.notna(row["midSellRatioMean"]) else 0
        low_buy_ratio_m = row["lowBuyRatioMean"] if "lowBuyRatioMean" in row and pd.notna(row["lowBuyRatioMean"]) else 0
        low_sell_ratio_m = row["lowSellRatioMean"] if "lowSellRatioMean" in row and pd.notna(row["lowSellRatioMean"]) else 0
        bet_buy_ratio_m = row["betBuyRatioMean"] if "betBuyRatioMean" in row and pd.notna(row["betBuyRatioMean"]) else 0
        bet_sell_ratio_m = row["betSellRatioMean"] if "betSellRatioMean" in row and pd.notna(row["betSellRatioMean"]) else 0
        avg_cash_ratio_m = row["avgCashRatioMean"] if "avgCashRatioMean" in row and pd.notna(row["avgCashRatioMean"]) else 0

        text = f"""
        우리 아이 나이대의 아이들 평균에 대한 정보입니다.
        평균 턴 체류시간은 {avg_stay_time_m}초, 
        평균 태그 턴 체류시간은 {tag_avg_stay_time_m}초 입니다.
        고위험 자산 평균 구매 비율은 {high_buy_ratio_m}%,
        고위험 자산 평균 판매 비율은 {high_sell_ratio_m}% 입니다.
        중위험 자산 평균 구매 비율은 {mid_buy_ratio_m}%,
        중위험 자산 평균 판매 비율은 {mid_sell_ratio_m}% 입니다.
        저위험 자산 평균 구매 비율은 {low_buy_ratio_m}%,
        저위험 자산 평균 판매 비율은 {low_sell_ratio_m}% 입니다.
        배팅 성공률은 각각
        매입 시기 예측 성공률은 {bet_buy_ratio_m}%고
        매각 시기 예측 성공률은 {bet_sell_ratio_m}% 입니다.
        평균 현금보유율은 {avg_cash_ratio_m} 입니다.
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

