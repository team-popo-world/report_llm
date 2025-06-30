def save_to_mongo(user_collection, graph_collection, user_id, response_user_json, graph_json):
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