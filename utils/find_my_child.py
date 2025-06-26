
def find_child_info(userId, client):
  response = client.retrieve(
      collection_name="my_user_summaries",
      ids=[userId] # ID 기반 조회
  )

  if response:
      point = response[0]
      print("✅ userId:", point.id)
      print("📌 요약 설명 (formatText):", point.payload.get("text"))
  else:
      print("해당 userId에 대한 데이터가 없습니다.")

  text = point.payload.get("text")

  return text