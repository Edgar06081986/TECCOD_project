from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import List, Optional
from opensearchpy import OpenSearch
import os
from faker import Faker
import random



app = FastAPI()

client = OpenSearch(
    hosts=[{"host": os.environ.get("OPENSEARCH_HOST", "opensearch"), "port": 9200, "scheme": "http"}],
    http_auth=(os.environ.get("OPENSEARCH_USER", "admin"), os.environ.get("OPENSEARCH_PASS", "admin")),
    use_ssl=False,
    verify_certs=False,
)

INDEX_NAME = "test_index"
CONTENT_TYPES = ["article", "news", "tutorial", "report"]

@app.post("/load_test_data")
def load_test_data(num_docs: int = 5):
    fake = Faker()
    docs = []
    for i in range(num_docs):
        doc = {
            "title": fake.sentence() + (" science" if i == 0 else ""),
            "content": fake.text(),
            "content_type": random.choice(CONTENT_TYPES),
        }
        docs.append(doc)
    actions = [
        {"_index": INDEX_NAME, "_source": doc}
        for doc in docs
    ]
    from opensearchpy import helpers
    helpers.bulk(client, actions)
    return {"inserted": len(docs)}

class SearchResult(BaseModel):
    title: str
    snippet: str

@app.get("/search", response_model=List[SearchResult])
def search(
    query: str = Query(..., description="Search query"),
    content_type: Optional[str] = Query(None, description="Content type filter")
):
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["title", "content"],
                        }
                    }
                ],
                "filter": [],
            }
        },
        "_source": ["title", "content"],
    }
    if content_type:
        body["query"]["bool"]["filter"].append({"term": {"content_type": content_type}})
    response = client.search(index=INDEX_NAME, body=body)
    results = []
    for hit in response["hits"]["hits"]:
        results.append({
            "title": hit["_source"]["title"],
            "snippet": hit["_source"]["content"][:50] + "...",
        })
    return results
