import ast

def get_graph_json(user_id, cluster_user_df, quest_df, shop_df):
    wanted_columns = [
        "daily_completion_rate_식탁 정리 도와주기",
        "daily_completion_rate_양치하기",
        "daily_completion_rate_이불 개기",
        "daily_completion_rate_장난감 정리하기",
        "daily_completion_rate_하루 이야기 나누기",
        "parent_completion_rate_STUDY",
        "parent_completion_rate_POPO",
        "parent_completion_rate_HABIT",
        "parent_completion_rate_ERRAND",
        "parent_completion_rate_HOUSEHOLD",
        "parent_completion_rate_ETC"
    ]
    # cluster_user_df로부터 invest_graph_data 생성 (비어 있을 경우 방어)
    if cluster_user_df.empty:
        print(f"📭 cluster_user_df가 비어있습니다. (userId: {user_id})")
        invest_graph_data = []
    else:
        invest_graph_data = cluster_user_df.drop(columns="userId").to_dict(orient="records")
    if quest_df.empty:
        print(f"📭 quest_df가 비어있습니다. (userId: {user_id})")
        quest_graph_data = []
    else:
        quest_df = quest_df.drop(columns="child_id")
        available_columns = [col for col in wanted_columns if col in quest_df.columns]
        quest_graph_data = quest_df[available_columns].to_dict(orient="records")
    if shop_df.empty:
        print(f"📭 shop_df가 비어있습니다. (userId: {user_id})")
        shop_graph_data = []
    else:
        shop_graph = shop_df["weeklyTrend"].iloc[0]
        shop_graph_data = ast.literal_eval(shop_graph)
    
    graph_json = {
            "invest_graph": invest_graph_data,
            "shop_graph": shop_graph_data,
            "quest_graph": quest_graph_data,
        }
    
    return graph_json