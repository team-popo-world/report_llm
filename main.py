from langchain_huggingface import HuggingFaceEmbeddings
from langchain.schema import Document
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance
from langchain_qdrant import QdrantVectorStore
from langchain_community.chat_models import ChatOpenAI
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from langchain.chains import LLMChain
from langchain_core.prompts import PromptTemplate
from utils.load_db import load_userId
from utils.invest_join import update_invest_data
from utils.quest_join import update_quest_data
from utils.shop_join import update_shop_data
from utils.find_my_child import find_child_info
from utils.cluster_count import transform_cluster_counts
from utils.json_to_natural_language import format_natural_language_summary
import requests
import pandas as pd
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import json
import re

# .env 파일 로드
load_dotenv(override=True)

# 환경변수 사용
url = os.getenv("QDRANT_URL")

client = QdrantClient(url=url, prefer_grpc=False)

# SQL에서 전체 데이터 받아오기
#user_list = load_userId()

user_list = ["237aac1b-4d6f-4ca9-9e4f-30719ea5967d", "d4af0657-f9db-40ff-babd-68681db7ddeb", "3ed8a159-adb0-4380-a648-e015cb82690a", "22b19e79-5822-46f9-8e3f-7e55e751f9dc", "d0a188a3-e24e-4772-95f7-07e59ce8885e", "4671ffa6-c77e-45f5-af8e-0f1d80804d86", "8a2b94b0-8395-41ad-99b0-6e8c0ea7edaa"]

# user_list = ["237aac1b-4d6f-4ca9-9e4f-30719ea5967d", "956f51a8-d6a0-4a12-a22b-9da3cdffc879", "f0220d43-513a-4619-973d-4ed84a42bf6a", "d0a188a3-e24e-4772-95f7-07e59ce8885e"]
# userId = "237aac1b-4d6f-4ca9-9e4f-30719ea5967d"

# invest_merged_df = pd.DataFrame()  # 초기 병합용 데이터프레임
# shop_merged_df = pd.DataFrame()

# for userId in user_list:
#    invest_merged_df = update_invest_data(userId, invest_merged_df)

# quest_merged_df = update_quest_data(user_list)
# shop_merged_df = update_shop_data(user_list)

# invest_merged_df.to_csv("data/invest_merged_df.csv", index=False)
# quest_merged_df.to_csv("data/quest_merged_df.csv", index=False)
#shop_merged_df.to_csv("data/shop_merged_df_test.csv", index=False)

invest_merged_df = pd.read_csv("data/invest_merged_df.csv")
quest_merged_df = pd.read_csv("data/quest_merged_df.csv")
shop_merged_df = pd.read_csv("data/shop_merged_df_test.csv")

cluster_df = transform_cluster_counts(invest_merged_df)
print(cluster_df.info())

# # 📌 사용 예시
#invest_merged_df.to_csv("data/invest_merged_df.csv", index=False)
#invest_merged_df = pd.read_csv("data/invest_merged_df.csv")
#quest_df = pd.read_csv("data/Final_Quest_DataFrame.csv")
#shop_df = pd.read_csv("data/shop_data_sample.csv")

# invest_merged_df = format_natural_language_summary(invest_merged_df)

# # 📁 저장
# formatted_df.to_csv("data/natural_format_data.csv", index=False)

# format_df = pd.read_csv("data/natural_format_data.csv")

# documents = [
#     Document(
#         page_content=row["formatText"],
#         metadata={"userId": row["userId"]}
#     )
#     for _, row in formatted_df.iterrows()
# ]

# Document(
#     page_content=f"""
#     사용자 ID: {row['userId']}
#     게임 시작 시각: {row['startedAt']}
#     요약 설명: {row['formatText']}
#     """.strip(),
#     metadata={"userId": row["userId"]}
# )

# embedding = HuggingFaceEmbeddings(model_name="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

# # 벡터 차원 알아내기
# embedding_dim = len(embedding.embed_query("임베딩 테스트"))

# client.delete_collection(collection_name="my_user_summaries")

# 조건부 생성 (기존 데이터 유지)
# if not client.collection_exists("my_user_summaries"):
#     client.create_collection(
#         collection_name="my_user_summaries",
#         vectors_config=VectorParams(size=embedding_dim, distance=Distance.COSINE)
#     )

# 문서 순회하며 업서트
# for doc in documents:
#     user_id = doc.metadata["userId"]
#     vector = embedding.embed_query(doc.page_content)
    
#     # Qdrant에 업서트
#     client.upsert(
#         collection_name="my_user_summaries",
#         points=[
#             PointStruct(
#                 id=user_id,  # 문자열 UUID를 그대로 ID로 사용
#                 vector=vector,
#                 payload={**doc.metadata, "text": doc.page_content}
#             )
#         ]
#     )

# Qdrant 벡터 DB 생성 및 저장
# qdrant = QdrantVectorStore(
#     client=client,
#     embedding=embedding,
#     collection_name="my_user_summaries"
# )

# qdrant.add_documents(documents)

# 내 아이의 userId
# target_user_id = "237aac1b-4d6f-4ca9-9e4f-30719ea5967d"

# Qdrant에서 해당 userId로 point 조회
# user_data = find_child_info(target_user_id, client)
# response = client.retrieve(
#     collection_name="my_user_summaries",
#     ids=[target_user_id] # ID 기반 조회
# )

template = """
너는 아이들의 게임 데이터를 분석하여 학습 행동을 파악하고, 각 영역별로 **구체적인 수치와 비교를 통해 피드백과 가이드를 제시하는 AI 학습 분석가**야.

분석 대상은 항상 다음의 세 가지 활동 영역이야:
- 투자(invest)
- 상점(shop)
- 퀘스트(quest)

각 영역에는 숫자 기반 지표들이 포함되어 있으니, **수치에 기반하여 객관적이고 구체적으로 설명**해줘. 예를 들어 '활동량이 2.0이고 전체 평균은 1.5'라면, '전체보다 약 33% 높은 활동량'이라고 서술하는 식이야.

각 영역마다 아래 항목을 모두 포함한 **요약 텍스트**를 생성해:
1. 활동량 또는 빈도 수치와 의미
2. 행동의 특성 (예: 고위험 선호, 소비 패턴, 성공률 등)
3. 전체 평균과 비교한 수치 및 해석
4. 개선할 점 (왜 개선이 필요한지도 간단히 설명)
5. 칭찬할 점 (수치를 기반으로 구체적으로 칭찬)

그리고 마지막으로, 모든 영역을 종합해서:
- 아이의 **전반적인 행동 경향**을 정리하고,
- **학습 및 재무 습관에 대한 종합 가이드라인**을 작성해줘.

🔹 출력 형식은 반드시 다음의 JSON 구조로 해줘. 설명 없이 JSON만 반환해.
```json
{{
  "userId": "{user_id}",
  "invest": "투자 영역 분석 결과 요약",
  "shop": "상점 영역 분석 결과 요약",
  "quest": "퀘스트 영역 분석 결과 요약",
  "all": "전체 행동 경향 및 학습 가이드 요약"
}}

[투자 활동 데이터]
{invest_data}

[상점 활동 데이터]
{shop_data}

[퀘스트 활동 데이터]
{quest_data}
"""

# [상점 활동 데이터]
# {shop_data}

# [퀘스트 활동 데이터]
# {quest_data}
"""
prompt = PromptTemplate.from_template(template)

openai_key = os.getenv("OPENAI_KEY")

# 모델 정의
llm = ChatOpenAI(
    model_name="gpt-4o-mini",
    streaming=True,
    temperature=0.8,
    openai_api_key=openai_key,
    callbacks=[StreamingStdOutCallbackHandler()]
)

chain = LLMChain(prompt=prompt, llm=llm)
"""



# response = chain.run({
#     "user_id": target_user_id,
#     "invest_data": invest_df,
#     "shop_data": shop_df,
#     "quest_data": quest_df
# })

# print("🧾 모델 응답 원문:", repr(response))

# # 1. Markdown 포맷 제거 (예: ```json\n ... \n```)
# cleaned = re.sub(r"^```(?:json)?\n|\n```$", "", response.strip())

# MongoDB 연결
uri = os.getenv("MONGO_URI")
db_name = os.getenv("MONGO_DB_NAME")

client = MongoClient(uri)  # 또는 Atlas URI
db = client[db_name]
user_collection = db["user_analysis"]
graph_collection = db["user_graph"]


# try:
#     response_json = json.loads(cleaned)
# except json.JSONDecodeError as e:
#     print("❗ JSON 디코딩 실패:", e)
#     response_json = {}

# # userId 중복 방지: 기존에 있으면 업데이트, 없으면 삽입
# if response_json:
#     user_id = response_json.get("userId")
#     collection.update_one(
#         {"userId": user_id},
#         {"$set": response_json},
#         upsert=True
#     )
#     print(f"✅ MongoDB에 저장 완료 (userId: {user_id})")

###########################################################################
# ✅ 전체 사용자에 대해 반복 실행
"""
for user_id in user_list:
    try:
        print(f"🚀 분석 중: {user_id}")

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
        quest_df = quest_merged_df[quest_merged_df["child_id"] == user_id]
        shop_df = shop_merged_df[shop_merged_df["userId"] == user_id] 

        # 체인 실행
        response = chain.run({
            "user_id": user_id,
            "invest_data": invest_df,
            "shop_data": shop_df,
            "quest_data": quest_df
        })

        # 출력 클린업
        cleaned = re.sub(r"^```(?:json)?\n|\n```$", "", response.strip())

        # JSON 파싱
        response_user_json = json.loads(cleaned)

        cluster_df.drop(columns="userId", inplace=True)
        quest_df.drop(columns="child_id", inplace=True)
        # quest_df = quest_df[["userId", "", "", "", ""]]
        shop_df = shop_df[["weeklyTrend"]]

        graph_json = {
            "invest_graph": cluster_df.to_dict(orient="records"),
            "shop_graph": shop_df.to_dict(orient="records"),
            "quest_graph": quest_df.to_dict(orient="records"),
        }

        # MongoDB에 저장
        user_collection.update_one(
            {"userId": user_id},
            {"$set": response_user_json},
            upsert=True
        )

        graph_collection.update_one(
            {"userId": user_id},
            {"$set": graph_json},
            upsert=True
        )

        print(f"✅ 저장 완료: {user_id}")

    except Exception as e:
        print(f"❗ 오류 발생 (userId={user_id}): {e}")

"""