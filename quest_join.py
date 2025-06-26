import pandas as pd
from functools import reduce
from quest_preprocess import process_completion_rate, process_approval_time, process_completion_reward, process_completion_time

# child_ids 예시 (실제 값으로 교체)
child_ids = ["8a2b94b0-8395-41ad-99b0-6e8c0ea7edaa", "d0a188a3-e24e-4772-95f7-07e59ce8885e", "2de604dc-68f7-45b7-ad0b-ad1a63a2fc0b", "8d8a3d15-4058-4e07-8ac6-2089f7f4c459", "237aac1b-4d6f-4ca9-9e4f-30719ea5967d", "ba924716-1f71-4735-a75c-4aa0a289841d"
]

# 1. 각각의 데이터프레임 생성
completion_rate_daily_df = process_completion_rate("daily", child_ids)

completion_time_daily_df = process_completion_time("daily", child_ids)
completion_time_parent_df = process_completion_time("parent", child_ids)

completion_reward_df = process_completion_reward(child_ids)  # parent만 있음

approval_time_daily_df = process_approval_time("daily", child_ids)
approval_time_parent_df = process_approval_time("parent", child_ids)

# 2. 데이터프레임 리스트로 모으기
dfs = [
    completion_rate_daily_df,
    completion_reward_df,
    approval_time_daily_df,
    approval_time_parent_df,
    completion_time_daily_df,
    completion_time_parent_df,
]

# 3. 빈 데이터프레임 제외 (API 에러 등으로 일부 없을 수 있음)
dfs = [df for df in dfs if not df.empty]

# 4. reduce로 child_id 기준 outer join
if dfs:
    final_df = reduce(lambda left, right: pd.merge(left, right, on="child_id", how="outer"), dfs)
else:
    final_df = pd.DataFrame()

# 5. 결과 확인
print(final_df)