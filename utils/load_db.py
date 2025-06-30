import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
from urllib.parse import quote_plus
from utils.invest_join import update_invest_data
from utils.cluster_count import update_cluster_data
from utils.quest_join import update_quest_data
from utils.shop_join import update_shop_data

load_dotenv(override=True)

# snake_case -> camelCase
def snake_to_camel(s):
    parts = s.split('_')
    return parts[0] + ''.join(word.capitalize() for word in parts[1:])

def load_userId():
  host=os.getenv("DB_HOST")
  port=os.getenv("DB_PORT")
  dbname=os.getenv("DB_NAME")
  user=os.getenv("DB_USER")
  password=quote_plus(os.getenv("DB_PASSWORD"))
  
  query = "SELECT user_id FROM users;"

  engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
  )
  
  df = pd.read_sql(query, engine)
  
  df["user_id"] = df["user_id"].astype(str)

  df.columns = [snake_to_camel(col) for col in df.columns]

  user_id_list = df["userId"].tolist()

  return user_id_list

def load_data(user_list):
  invest_merged_df = pd.DataFrame()
  shop_merged_df = pd.DataFrame()

  for userId in user_list:
    invest_merged_df = update_invest_data(userId, invest_merged_df)

  quest_merged_df = update_quest_data(user_list)
  shop_merged_df = update_shop_data(user_list)
  cluster_df = update_cluster_data(user_list)

  return invest_merged_df, quest_merged_df, shop_merged_df, cluster_df