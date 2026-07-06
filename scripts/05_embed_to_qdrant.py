# scripts/05_embed_to_qdrant.py
"""Integration 5: Data → Embeddings → Qdrant.

Ưu tiên gọi embedding service trên Kaggle (EMBED_NGROK_URL). Nếu tunnel
chưa có / mất kết nối → fallback sang local pseudo-embedding (hash-based,
deterministic) để pipeline vẫn chạy end-to-end được.
"""
import hashlib
import math
import os

import requests
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

try:  # load .env nếu có
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

EMBED_URL = os.environ.get("EMBED_NGROK_URL", "").rstrip("/")
VECTOR_SIZE = 384

qdrant = QdrantClient(host="localhost", port=6333)

# Tạo collection
qdrant.recreate_collection(
    collection_name="documents",
    vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
)


def local_pseudo_embedding(text: str) -> list[float]:
    """Fallback embedding: deterministic, normalized — chỉ dùng khi Kaggle offline."""
    vec = []
    for i in range(VECTOR_SIZE):
        h = hashlib.sha256(f"{text}:{i}".encode()).digest()
        vec.append(int.from_bytes(h[:4], "big") / 2**32 - 0.5)
    norm = math.sqrt(sum(v * v for v in vec))
    return [v / norm for v in vec]


def get_embeddings(texts: list[str]) -> list[list[float]]:
    if EMBED_URL:
        try:
            response = requests.post(
                f"{EMBED_URL}/embed", json={"texts": texts}, timeout=30
            )
            response.raise_for_status()
            print(f"Embedded {len(texts)} texts via Kaggle service")
            return response.json()["embeddings"]
        except Exception as e:
            print(f"[WARN] Kaggle embedding service unavailable ({e}), using local fallback")
    else:
        print("[WARN] EMBED_NGROK_URL not set, using local fallback embeddings")
    return [local_pseudo_embedding(t) for t in texts]


def embed_and_store(records: list[dict]):
    embeddings = get_embeddings([r["text"] for r in records])

    points = [
        PointStruct(id=i, vector=emb, payload=rec)
        for i, (emb, rec) in enumerate(zip(embeddings, records))
    ]
    qdrant.upsert(collection_name="documents", points=points)
    print(f"Integration 5 OK: {len(points)} vectors stored in Qdrant")


# Test với sample data
embed_and_store([
    {"id": "doc_001", "text": "AI platform integration test"},
    {"id": "doc_002", "text": "Kafka to Airflow pipeline"},
])
