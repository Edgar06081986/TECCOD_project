from opensearchpy import OpenSearch, helpers
from faker import Faker
import random

# Подключение к OpenSearch
client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    http_auth=("admin", "admin"),  # в реальном проекте используйте .env!
    use_ssl=False,
    verify_certs=False,
)

# Настройки индекса
INDEX_NAME = "test_index"
CONTENT_TYPES = ["article", "news", "tutorial", "report"]

# Создаём индекс
def create_index():
    if not client.indices.exists(index=INDEX_NAME):
        body = {
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "content": {"type": "text"},
                    "content_type": {"type": "keyword"},  # для точного совпадения
                }
            }
        }
        client.indices.create(index=INDEX_NAME, body=body)
        print(f"Индекс {INDEX_NAME} создан.")
    else:
        print(f"Индекс {INDEX_NAME} уже существует.")

# Генерация тестовых данных
def generate_data(num_docs=5):
    fake = Faker()
    docs = []
    for _ in range(num_docs):
        doc = {
            "title": fake.sentence(),
            "content": fake.text(),
            "content_type": random.choice(CONTENT_TYPES),
        }
        docs.append(doc)
    return docs

# Загрузка данных в OpenSearch
def upload_data(docs):
    actions = [
        {"_index": INDEX_NAME, "_source": doc}
        for doc in docs
    ]
    helpers.bulk(client, actions)
    print(f"Загружено {len(docs)} документов.")

# Поиск по ключевому слову с фильтром по content_type
def search(query, content_type=None):
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
        body["query"]["bool"]["filter"].append(
            {"term": {"content_type": content_type}}
        )

    response = client.search(index=INDEX_NAME, body=body)
    results = []
    for hit in response["hits"]["hits"]:
        results.append({
            "title": hit["_source"]["title"],
            "snippet": hit["_source"]["content"][:50] + "...",
        })
    return results

if __name__ == "__main__":
    create_index()
    docs = generate_data()
    upload_data(docs)

    # Пример поиска
    query = "science"  # тестовый запрос
    content_type = "article"  # фильтр (None если не нужен)
    results = search(query, content_type)
    print(f"Результаты поиска для '{query}':")
    for res in results:
        print(f"- {res['title']}: {res['snippet']}")