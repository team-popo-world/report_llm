from langchain_community.chat_models import ChatOpenAI
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from langchain.chains import LLMChain
from langchain_core.prompts import PromptTemplate
import os
import re
import json

def get_llm_chain():
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

  return LLMChain(prompt=prompt, llm=llm)

def get_response(user_id, chain, invest_df, quest_df, shop_df):
  if invest_df.empty and quest_df.empty and shop_df.empty:
    print(f"📭 모든 활동 데이터가 비어 있습니다. (userId: {user_id})")
    return {
        "userId": user_id,
        "invest": "분석할 투자 데이터가 없습니다.",
        "shop": "분석할 상점 데이터가 없습니다.",
        "quest": "분석할 퀘스트 데이터가 없습니다.",
        "all": "모든 영역에 대한 데이터가 없어 분석을 진행할 수 없습니다. 활동 데이터가 쌓인 후 다시 시도해주세요."
    }
  try:
    # 체인 실행
    response = chain.run({
        "user_id": user_id,
        "invest_data": invest_df,
        "shop_data": shop_df,
        "quest_data": quest_df
    })

    # 출력 클린업 및 JSON 파싱
    cleaned = re.sub(r"^```(?:json)?\n|\n```$", "", response.strip())
    response_user_json = json.loads(cleaned)

    return response_user_json
  
  except Exception as e:
    print(f"[ERROR] LLM 응답 처리 중 오류 발생 (userId={user_id}): {e}")
    return {
        "userId": user_id,
        "invest": "분석 중 오류가 발생했습니다.",
        "shop": "분석 중 오류가 발생했습니다.",
        "quest": "분석 중 오류가 발생했습니다.",
        "all": "분석 과정에서 문제가 발생하여 요약을 생성할 수 없습니다."
    }