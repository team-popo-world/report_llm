from pymongo import MongoClient
from dotenv import load_dotenv
import os

def load_mongo(collection_name: str):
  load_dotenv(override=True)

  client = MongoClient(os.getenv("MONGO_URI"))
  db = client[os.getenv("MONGO_DB_NAME")]
  collection = db[collection_name]

  cursor = collection.find({})
  json_data_list = list(cursor)

  return json_data_list