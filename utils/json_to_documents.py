from langchain.schema import Document
from utils.json_to_natural_language import json_to_natural_language
from typing import List, Dict

def json_to_documents(json_data_list: List[Dict]):
    documents = []
    for record in json_data_list:
        page_text = json_to_natural_language(record)
        metadata = {
            "userId": record.get("userId"),
            "startedAt": record.get("startedAt"),
        }
        documents.append(Document(page_content=page_text, metadata=metadata))
    return documents