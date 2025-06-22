def json_to_natural_language(json_obj):
    user_id = json_obj["userId"]
    started_at = json_obj["startedAt"]
    entries = []
    for item in json_obj["data"]:
        entries.append(f'{item["turn"]}턴에서 {item["riskLevel"]} 위험 자산을 '
                       f'{item["buyCount"]}회 매수하고 {item["sellCount"]}회 매도')
    summary = f'{user_id}는 {started_at}에 시뮬레이션을 시작했으며, ' + ", ".join(entries) + '했습니다.'
    return summary

# def json_to_natural_language(record: Dict) -> str:
#     user_id = record.get("userId", "unknown user")
#     started_at = record.get("startedAt", "unknown time")
#     data = record.get("data", [])

#     text = f"사용자 {user_id}는 {started_at}부터 다음과 같은 투자 활동을 했습니다:\n"
#     for d in data:
#         turn = d.get("turn", "N/A")
#         buy = d.get("buy", 0)
#         sell = d.get("sell", 0)
#         text += f"- {turn}턴: {buy}원을 매수하고 {sell}원을 매도했습니다.\n"

#     return text.strip()