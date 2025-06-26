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
from utils.call_api import update_data
from utils.find_my_child import find_child_info
from utils.json_to_natural_language import format_natural_language_summary
import requests
import pandas as pd
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv(override=True)

# 환경변수 사용
url = os.getenv("QDRANT_URL")

client = QdrantClient(url=url, prefer_grpc=False)

# SQL에서 전체 데이터 받아오기
# user_list = load_userId()

# print(len(user_list))

user_list = ["237aac1b-4d6f-4ca9-9e4f-30719ea5967d", "956f51a8-d6a0-4a12-a22b-9da3cdffc879", "f0220d43-513a-4619-973d-4ed84a42bf6a", "d0a188a3-e24e-4772-95f7-07e59ce8885e"]

# api로 그래프 별로 정보 가져오기
graph_list = ["avg_stay_time", "buy_ratio", "sell_ratio", "buy_sell_ratio", "bet_ratio", "avg_cash_ratio"]
# userId = "956f51a8-d6a0-4a12-a22b-9da3cdffc879"

invest_merged_df = pd.DataFrame()  # 초기 병합용 데이터프레임

# for userId in user_list:
#     update_data(userId, invest_merged_df)

# invest 쪽 api 불러오기
# for userId in user_list:
#     user_df = pd.DataFrame()
#     for graphName in graph_list:
#             invest_url = f"http://43.203.175.69:8002/api/invest/{graphName}/week?userId={userId}"
#             headers = {"Content-Type": "application/json"}

#             response = requests.get(invest_url, headers=headers)

#             if response.status_code == 200:
#                 # print(response.json())  # 응답 데이터
#                 data = response.json()  # JSON -> Python 객체 (list of dict)
#                 df = pd.DataFrame(data)  # 리스트를 바로 DataFrame으로 변환
#                 print(df.head())  # 확인용
#                 if df.empty:
#                     print(f"📭 [EMPTY] userId: {userId}, graph: {graphName}")
#                     continue  # 아무것도 안하고 다음으로

#                 print(f"✅ [RECEIVED] userId: {userId}, graph: {graphName}, rows: {len(df)}")

#                 # 병합 처리
#                 if user_df.empty:
#                     user_df = df
#                 else:
#                     user_df = pd.merge(user_df, df, on=["userId", "startedAt"], how="outer")

#             else:
#                 print(f"❌ [ERROR] userId: {userId}, graph: {graphName}")
#                 print("상태 코드:", response.status_code)

#                 # 응답이 JSON 형식이면 파싱
#                 try:
#                     error_data = response.json()
#                     print("🔍 에러 응답(JSON):", error_data)
#                 except ValueError:
#                     # JSON 형식이 아니면 텍스트 그대로 출력
#                     print("🔍 에러 응답(text):", response.text)
    
#     if not user_df.empty:
#         invest_merged_df = pd.concat([invest_merged_df, user_df], ignore_index=True)

# invest_merged_df = pd.read_csv("data/특정사람_invest_api_불러와서_병합.csv")

# # 📌 사용 예시
# invest_merged_df.to_csv("data/invest_merged_df.csv", index=False)
invest_merged_df = pd.read_csv("data/invest_merged_df.csv")
formatted_df = format_natural_language_summary(invest_merged_df)

# # 📁 저장
# formatted_df.to_csv("data/natural_format_data.csv", index=False)

# format_df = pd.read_csv("data/natural_format_data.csv")

documents = [
    Document(
        page_content=row["formatText"],
        metadata={"userId": row["userId"]}
    )
    for _, row in formatted_df.iterrows()
]

# Document(
#     page_content=f"""
#     사용자 ID: {row['userId']}
#     게임 시작 시각: {row['startedAt']}
#     요약 설명: {row['formatText']}
#     """.strip(),
#     metadata={"userId": row["userId"]}
# )

embedding = HuggingFaceEmbeddings(model_name="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

# 벡터 차원 알아내기
embedding_dim = len(embedding.embed_query("임베딩 테스트"))

# client.delete_collection(collection_name="my_user_summaries")

# 조건부 생성 (기존 데이터 유지)
if not client.collection_exists("my_user_summaries"):
    client.create_collection(
        collection_name="my_user_summaries",
        vectors_config=VectorParams(size=embedding_dim, distance=Distance.COSINE)
    )

# 문서 순회하며 업서트
for doc in documents:
    user_id = doc.metadata["userId"]
    vector = embedding.embed_query(doc.page_content)
    
    # Qdrant에 업서트
    client.upsert(
        collection_name="my_user_summaries",
        points=[
            PointStruct(
                id=user_id,  # 문자열 UUID를 그대로 ID로 사용
                vector=vector,
                payload={**doc.metadata, "text": doc.page_content}
            )
        ]
    )

# Qdrant 벡터 DB 생성 및 저장
qdrant = QdrantVectorStore(
    client=client,
    embedding=embedding,
    collection_name="my_user_summaries"
)

# qdrant.add_documents(documents)

# 내 아이의 userId
target_user_id = "237aac1b-4d6f-4ca9-9e4f-30719ea5967d"

# Qdrant에서 해당 userId로 point 조회
# user_data = find_child_info(target_user_id, client)
# response = client.retrieve(
#     collection_name="my_user_summaries",
#     ids=[target_user_id] # ID 기반 조회
# )

invest_df = invest_merged_df[invest_merged_df["userId"] == target_user_id]
print(invest_df)

template = """
너는 아이들의 게임 데이터를 분석하여 학습 행동을 파악하고, 각 영역별로 피드백과 가이드를 제시하는 AI 학습 분석가야.

너는 항상 다음의 세 가지 활동 영역을 기준으로 분석해:
- 투자(invest)
- 상점(shop)
- 퀘스트(quest)

각 영역별로 제공된 데이터만을 기반으로 다음을 포함하는 **요약 텍스트**를 각각 만들어줘:
1. 활동량 또는 빈도
2. 행동의 특성 (예: 고위험 선호, 자주 소비 등)
3. 전체 평균과의 비교
4. 개선할 점
5. 칭찬할 점

마지막으로, 모든 영역을 종합한 분석 요약과 아이에게 적합한 학습/투자 습관 가이드를 생성해줘.

🔹 출력 형식은 반드시 다음의 JSON 구조로 해줘:
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

prompt = PromptTemplate.from_template(template)

# 모델 정의
llm = ChatOpenAI(
    model_name="gpt-4o-mini",
    streaming=True,
    temperature=0.8,
    callbacks=[StreamingStdOutCallbackHandler()]
)

chain = LLMChain(prompt=prompt, llm=llm)

response = chain.run({
    "user_id": target_user_id,
    "invest_data": invest_df,
    "shop_data": shop_df,
    "quest_data": quest_df
})

print(response)