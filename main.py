from langchain_huggingface import HuggingFaceEmbeddings
from langchain.schema import Document
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance
from langchain_qdrant import QdrantVectorStore
from utils.load_db import load_userId
from utils.json_to_natural_language import format_natural_language_summary
# from utils.json_to_documents import json_to_documents
import requests
import pandas as pd
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv(override=True)

# 환경변수 사용
host = os.getenv("QDRANT_HOST")
port = os.getenv("QDRANT_PORT")
url = os.getenv("QDRANT_URL")
# port = os.getenv("QDRANT_PORT")

# print(f"Qdrant 연결: {host}: {port}")

# ❶ Qdrant 서버 연결 (서버 주소를 바꾸세요)

client = QdrantClient(url=url, prefer_grpc=False)

# 참고자료 dag를 이용해서 우리 아이의 결과를 중심으로 레포트 작성하기

# 전체 유저에 대해서 update해야 하므로 sql에서 userId만 불러와서 저장
# userId(str)
# report_data(json)

# SQL에서 전체 데이터 받아오기
user_list = load_userId()

print(len(user_list))

"""
데이터 프레임
# userId
# type(str, 어떤 분야인지( ex)invest, quest, ...))
# graph_name(str; 어떤 그래프인지)
# graph_data(json; 그래프 그리는데 사용한 json파일)
# graph_summary(str; 그래프 포맷팅한 결과)
"""

# api로 그래프 별로 정보 가져오기
graph_list = ["avg_stay_time", "buy_ratio", "sell_ratio", "buy_sell_ratio", "bet_ratio", "avg_cash_ratio"]

invest_merged_df = pd.DataFrame()  # 초기 병합용 데이터프레임

##########################################테스트용#############################################
# userId = "956f51a8-d6a0-4a12-a22b-9da3cdffc879"
# for graphName in graph_list:
#     invest_url = f"http://43.203.175.69:8002/api/invest/{graphName}/week?userId={userId}"
#     headers = {"Content-Type": "application/json"}

#     response = requests.get(invest_url, headers=headers)

#     if response.status_code == 200:
#         # print(response.json())  # 응답 데이터
#         data = response.json()  # JSON -> Python 객체 (list of dict)
#         df = pd.DataFrame(data)  # 리스트를 바로 DataFrame으로 변환
#         print(df.head())  # 확인용
#         if df.empty:
#             print(f"📭 [EMPTY] userId: {userId}, graph: {graphName}")
#             continue  # 아무것도 안하고 다음으로

#         print(f"✅ [RECEIVED] userId: {userId}, graph: {graphName}, rows: {len(df)}")

#         # 병합 처리
#         if invest_merged_df.empty:
#             invest_merged_df = df
#         else:
#             invest_merged_df = pd.merge(invest_merged_df, df, on=["userId", "startedAt"], how="inner")

#     else:
#         print("에러 발생:", response.status_code)
#         print("에러 내용:", response.text)

# print(invest_merged_df.head())
# print("invest_merged_df columns:", invest_merged_df.columns)
# invest_merged_df.to_csv("data/특정사람_invest_api_불러와서_병합.csv", index=False, encoding="utf-8")
#####################################################################################################

# invest 쪽 api 불러오기
for userId in user_list:
    for graphName in graph_list:
            invest_url = f"http://43.203.175.69:8002/api/invest/{graphName}/week?userId={userId}"
            headers = {"Content-Type": "application/json"}

            response = requests.get(invest_url, headers=headers)

            if response.status_code == 200:
                # print(response.json())  # 응답 데이터
                data = response.json()  # JSON -> Python 객체 (list of dict)
                df = pd.DataFrame(data)  # 리스트를 바로 DataFrame으로 변환
                print(df.head())  # 확인용
                if df.empty:
                    print(f"📭 [EMPTY] userId: {userId}, graph: {graphName}")
                    continue  # 아무것도 안하고 다음으로

                print(f"✅ [RECEIVED] userId: {userId}, graph: {graphName}, rows: {len(df)}")

                # 병합 처리
                if invest_merged_df.empty:
                    invest_merged_df = df
                else:
                    invest_merged_df = pd.merge(invest_merged_df, df, on=["userId", "startedAt"], how="inner")

            else:
                print("에러 발생:", response.status_code)
                print("에러 내용:", response.text)

# quest_url = f"http://43.203.175.69:8000/graph/{}/{}?userId={userId}"
# shop_url = f"http://43.203.175.69:8001/api/{}/{}?userId={userId}"

# invest_merged_df = pd.read_csv("data/특정사람_invest_api_불러와서_병합.csv")

# # 📌 사용 예시
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

query = "고위험 자산 선호하는 아이들"
results = qdrant.similarity_search(query, k=5)

for i, doc in enumerate(results, start=1):
    print(f"\n📌 [TOP {i}]")
    print("🔍 userId:", doc.metadata.get("userId", "없음"))
    print("🧾 내용:", doc.page_content.strip()[:300]) 

# json = load_mongo(collection_name="graph1_all_history")

# documents = json_to_documents(json)

# # 벡터 DB에 저장
# vector_db = Chroma.from_documents(
#   documents, 
#   embeddings=embeddings, 
#   persist_directory="./chroma_db"
# )

# # 이후 RAG에 활용
# retriever = vector_db.as_retriever()
# retriever.get_relevant_documents("최근 투자 트렌드는?")