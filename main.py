# from langchain.embeddings import OpenAIEmbeddings
# from langchain.vectorstores import Chroma
from utils.load_db import load_userId
from utils.json_to_natural_language import format_natural_language_summary
# from utils.json_to_documents import json_to_documents
import requests
import pandas as pd

# 참고자료 dag를 이용해서 우리 아이의 결과를 중심으로 레포트 작성하기

# 전체 유저에 대해서 update해야 하므로 sql에서 userId만 불러와서 저장
# userId(str)
# report_data(json)

# SQL에서 전체 데이터 받아오기
user_list = load_userId()

print(user_list)

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

# invest_merged_df = pd.DataFrame()  # 초기 병합용 데이터프레임

##########################################지울 부분#############################################
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
# for userId in user_list:
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
#                 if merged_df.empty:
#                     merged_df = df
#                 else:
#                     merged_df = pd.merge(merged_df, df, on=["userId", "startedAt"], how="inner")

#             else:
#                 print("에러 발생:", response.status_code)
#                 print("에러 내용:", response.text)

invest_merged_df = pd.read_csv("data/특정사람_invest_api_불러와서_병합.csv")

# 📌 사용 예시
formatted_df = format_natural_language_summary(invest_merged_df)

# 📁 저장
formatted_df.to_csv("data/natural_format_data.csv", index=False)

            

# quest_url = f"http://43.203.175.69:8000/graph/{}/{}?userId={userId}"
# shop_url = f"http://43.203.175.69:8001/api/{}/{}?userId={userId}"

# embeddings = OpenAIEmbeddings()

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