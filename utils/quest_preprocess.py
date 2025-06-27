import requests
import pandas as pd


# API 호출 함수
def fetch_graph_data(
    child_id: str,
    quest_type: str,
    graph_name: str,
    period: str = "recent7",
) -> dict:

    url = f"http://43.203.175.69:8000/graph/{quest_type}/{graph_name}?childId={child_id}&period={period}"
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # status_code != 200이면 에러 발생
        return response.json()
    except requests.RequestException as e:
        print(f"❗️API 요청 실패: {url}\n{e}")
        return None
    
# Completion_rate 함수
def process_completion_rate(quest_type, child_ids):
    dfs = []
    for child_id in child_ids:
        json_data = fetch_graph_data(child_id, quest_type, "completion_rate")
        # result가 list가 아니면(빈문자열, None 등) child_id만
        if (not json_data or
            "result" not in json_data or
            not isinstance(json_data["result"], list) or
            not json_data["result"] or
            any([not isinstance(q, dict) for q in json_data["result"]])
        ):
            dfs.append(pd.DataFrame([{"child_id": child_id}]))
            continue
        cid = json_data['childid']
        result = json_data['result']
        row_dict = {"child_id": cid}
        for quest in result:
            if not isinstance(quest, dict):
                continue
            if quest_type == "parent":
                quest_name = quest["label"]
            else:
                quest_name = quest["questName"]
            col_name = f"{quest_type}_completion_rate_{quest_name}"
            row_dict[col_name] = quest["completion_rate"]
        dfs.append(pd.DataFrame([row_dict]))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# Completion_time 함수
def process_completion_time(quest_type, child_ids):
    dfs = []
    for child_id in child_ids:
        json_data = fetch_graph_data(child_id, quest_type, "completion_time")
        # result가 정상인지 체크
        if (
            not json_data or
            "result" not in json_data or
            not isinstance(json_data["result"], list) or
            not json_data["result"] or
            any([not isinstance(q, dict) for q in json_data["result"]])
        ):
            # 비정상(빈문자열, None, 기타) → 컬럼은 child_id만, 나머지 null
            dfs.append(pd.DataFrame([{"child_id": child_id}]))
            continue
        cid = json_data['childid']
        result = json_data['result']
        row_dict = {"child_id": cid}
        for quest in result:
            if not isinstance(quest, dict):
                continue
            if quest_type == "parent":
                quest_name = quest["label"]
            else:
                quest_name = quest["quest_name"]
            for dist in quest.get("distribution", []):
                time_bin = dist.get("time_bin")
                count = dist.get("count")
                col_name = f"{quest_type}_completion_time_{quest_name}_{time_bin}"
                row_dict[col_name] = count
        dfs.append(pd.DataFrame([row_dict]))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# Completion_reward 함수
def process_completion_reward(child_ids):
    dfs = []
    for child_id in child_ids:
        json_data = fetch_graph_data(child_id, "parent", "completion_reward")
        if (not json_data or
            "result" not in json_data or
            not isinstance(json_data["result"], list) or
            not json_data["result"] or
            any([not isinstance(q, dict) for q in json_data["result"]])
        ):
            dfs.append(pd.DataFrame([{"child_id": child_id}]))
            continue
        cid = json_data['childid']
        result = json_data['result']
        row_dict = {"child_id": cid}
        for quest in result:
            if not isinstance(quest, dict):
                continue
            label = quest.get("label", "unknown")
            reward = quest.get("reward")
            rate = quest.get("completion_rate")
            row_dict[f"parent_completion_reward_{label}"] = reward
            row_dict[f"parent_completion_rate_{label}"] = rate
        dfs.append(pd.DataFrame([row_dict]))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# Approval_time 함수
def process_approval_time(quest_type, child_ids):
    dfs = []
    for child_id in child_ids:
        json_data = fetch_graph_data(child_id, quest_type, "approval_time")
        if (not json_data or
            "result" not in json_data or
            not isinstance(json_data["result"], dict)  # approval_time만 dict!
        ):
            dfs.append(pd.DataFrame([{"child_id": child_id}]))
            continue
        cid = json_data['childid']
        formatted = json_data["result"].get("formatted")
        col_name = f"{quest_type}_approval_time"
        row_dict = {
            "child_id": cid,
            col_name: formatted
        }
        dfs.append(pd.DataFrame([row_dict]))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()