from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Chroma
from utils.mongo_db import load_mongo
from utils.json_to_documents import json_to_documents
import requests

# 전체 유저에 대해서 update해야 하므로 sql에서 userId만 불러와서 저장
# userId(str)
# report_data(json)

type = ""
graph_name = ""
userId = ""

for type in []:
    for graph_name in []:
        for userId in []:
            type = ""

url = f"http://15.164.94.158:8002/api/{type}/{graph_name}/week?userId={userId}"
headers = {
    "Content-Type": "application/json"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    print(response.json())  # 응답 데이터
else:
    print("에러 발생:", response.status_code)


embeddings = OpenAIEmbeddings()

json = load_mongo(collection_name="graph1_all_history")

documents = json_to_documents(json)

# 벡터 DB에 저장
vector_db = Chroma.from_documents(
  documents, 
  embeddings=embeddings, 
  persist_directory="./chroma_db"
)

# 이후 RAG에 활용
retriever = vector_db.as_retriever()
retriever.get_relevant_documents("최근 투자 트렌드는?")