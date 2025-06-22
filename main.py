from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Chroma
from utils.mongo_db import load_mongo
from utils.json_to_documents import json_to_documents

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