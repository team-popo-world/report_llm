from langchain_community.chat_models import ChatOpenAI
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from langchain.chains import LLMChain
from langchain_core.prompts import PromptTemplate
import os
from utils.load_db import load_userId, load_data
from utils.graph import get_graph_json
from utils.llm import get_llm_chain, get_response
from utils.save_to_mongo import save_to_mongo
import pandas as pd
import uuid
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv(override=True)

# SQL에서 전체 데이터 받아오기
user_list = load_userId()
# invest_merged_df, quest_merged_df, shop_merged_df, cluster_df = load_data(user_list)

# user_list = ["237aac1b-4d6f-4ca9-9e4f-30719ea5967d", "d4af0657-f9db-40ff-babd-68681db7ddeb", "3ed8a159-adb0-4380-a648-e015cb82690a", "22b19e79-5822-46f9-8e3f-7e55e751f9dc", "d0a188a3-e24e-4772-95f7-07e59ce8885e", "4671ffa6-c77e-45f5-af8e-0f1d80804d86", "8a2b94b0-8395-41ad-99b0-6e8c0ea7edaa"]

# invest_merged_df.to_csv("data/invest_merged_df.csv", index=False)
# quest_merged_df.to_csv("data/quest_merged_df.csv", index=False)
# shop_merged_df.to_csv("data/shop_merged_df_test.csv", index=False)
# cluster_df.to_csv("data/cluster_merged_df.csv", index=False)

# invest_merged_df = pd.read_csv("data/invest_merged_df.csv")
# quest_merged_df = pd.read_csv("data/quest_merged_df.csv")
# shop_merged_df = pd.read_csv("data/shop_merged_df_test.csv")
# cluster_df = pd.read_csv("data/cluster_merged_df.csv")

# MongoDB 연결
uri = os.getenv("MONGO_URI")
db_name = os.getenv("MONGO_DB_NAME")

client = MongoClient(uri) 
db = client[db_name]
user_collection = db["user_analysis"]
graph_collection = db["user_graph"]

chain = get_llm_chain()

# ✅ 전체 사용자에 대해 반복 실행
for user_id in user_list:
    try:
        print(f"🚀 분석 중: {user_id}")
        
        isinstance(user_id, uuid.UUID)
        # 컬럼 존재 여부 먼저 확인
        for df_name, df, col in [
            ("invest_merged_df", invest_merged_df, "userId"),
            ("cluster_df", cluster_df, "userId"),
            ("quest_merged_df", quest_merged_df, "child_id"),
            ("shop_merged_df", shop_merged_df, "userId")
        ]:
            if col not in df.columns:
                print(f"❗ 오류: {df_name}에 '{col}' 컬럼이 없습니다. (userId={user_id})")
                raise KeyError(f"{df_name} missing column {col}")

        invest_df = invest_merged_df[invest_merged_df["userId"] == user_id]
        cluster_user_df = cluster_df[cluster_df["userId"] == user_id]
        invest_df = pd.merge(invest_df, cluster_user_df, on="userId", how="left")
        quest_df = quest_merged_df[quest_merged_df["child_id"] == user_id]
        shop_df = shop_merged_df[shop_merged_df["userId"] == user_id] 

        response_user_json = get_response(user_id, chain, invest_df, quest_df, shop_df)

        graph_json = get_graph_json(user_id, cluster_user_df, quest_df, shop_df)

        # MongoDB에 저장
        save_to_mongo(user_collection, graph_collection, user_id, response_user_json, graph_json)

    except Exception as e:
        print(f"❗ 오류 발생 (userId={user_id}): {e}")