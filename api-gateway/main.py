# api-gateway/main.py
"""AI Platform API Gateway.

Integration 8:  vLLM serving (Kaggle, qua ngrok/cloudflared) → API Gateway
Integration 9:  Prometheus metrics (prometheus-fastapi-instrumentator)
Integration 10: LangSmith tracing (bật khi có LANGCHAIN_API_KEY)

Graceful degradation:
- Qdrant down            → trả lời không có context (RAG tắt tạm thời)
- vLLM/Kaggle mất kết nối → trả về cached answer (Redis) hoặc fallback message
"""
import json
import os
import time
from typing import Optional

import httpx
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field

app = FastAPI(title="AI Platform API Gateway")
Instrumentator().instrument(app).expose(app)  # Integration 9: Prometheus

VLLM_URL = os.environ.get("VLLM_URL", "").rstrip("/")
EMBED_URL = os.environ.get("EMBED_URL", "").rstrip("/")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
MODEL_NAME = os.environ.get("MODEL_NAME", "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4")

FALLBACK_ANSWER = (
    "The LLM backend (Kaggle GPU) is currently unreachable. "
    "This is a cached/fallback response — please retry once the tunnel is restored."
)

# Redis dùng làm response cache cho fallback (Integration: graceful degradation)
try:
    import redis as _redis

    _cache = _redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=2)
except Exception:
    _cache = None

# Integration 10: LangSmith tracing — chỉ bật khi có API key
if os.environ.get("LANGCHAIN_API_KEY"):
    os.environ.setdefault("LANGSMITH_TRACING", "true")
    os.environ.setdefault("LANGCHAIN_TRACING_V2", "true")
    from langsmith import traceable
else:
    def traceable(*d_args, **d_kwargs):  # no-op decorator khi không có key
        def wrap(fn):
            return fn
        return wrap(d_args[0]) if d_args and callable(d_args[0]) else wrap


class ChatRequest(BaseModel):
    query: str = Field(..., min_length=1)
    embedding: Optional[list[float]] = None


async def get_embedding(query: str, provided: Optional[list[float]]) -> list[float]:
    """Dùng embedding client gửi lên; nếu không có thì gọi embedding service trên Kaggle."""
    if provided:
        return provided
    if EMBED_URL:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(f"{EMBED_URL}/embed", json={"texts": [query]})
                resp.raise_for_status()
                return resp.json()["embeddings"][0]
        except Exception as e:
            print(f"[WARN] Embedding service unavailable: {e}")
    return [0.0] * 384


async def vector_search(embedding: list[float]) -> list:
    """Integration 5: tìm context trong Qdrant. Qdrant down → trả context rỗng."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(
                f"{QDRANT_URL}/collections/documents/points/search",
                json={"vector": embedding, "limit": 3, "with_payload": True},
            )
            resp.raise_for_status()
            return resp.json().get("result", [])
    except Exception as e:
        print(f"[WARN] Qdrant unavailable, serving without context: {e}")
        return []


async def call_llm(prompt: str) -> Optional[dict]:
    """Gọi vLLM trên Kaggle. Mất kết nối → None để kích hoạt fallback."""
    if not VLLM_URL:
        return None
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{VLLM_URL}/v1/chat/completions",
                json={
                    "model": MODEL_NAME,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 512,
                },
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        print(f"[WARN] vLLM backend unreachable: {e}")
        return None


def cache_get(query: str) -> Optional[str]:
    if _cache is None:
        return None
    try:
        return _cache.get(f"answer_cache:{query}")
    except Exception:
        return None


def cache_set(query: str, answer: str):
    if _cache is None:
        return
    try:
        _cache.set(f"answer_cache:{query}", answer, ex=3600)
    except Exception:
        pass


@traceable(name="chat-pipeline")
async def run_chat_pipeline(query: str, embedding: Optional[list[float]]) -> dict:
    emb = await get_embedding(query, embedding)
    context = await vector_search(emb)

    context_texts = [
        p.get("payload", {}).get("text", "") for p in context if isinstance(p, dict)
    ]
    prompt = f"Context: {json.dumps(context_texts)}\n\nQuery: {query}"

    result = await call_llm(prompt)
    if result is not None:
        answer = result["choices"][0]["message"]["content"]
        cache_set(query, answer)
        return {"answer": answer, "model": result.get("model", MODEL_NAME), "degraded": False}

    # Fallback path: cached answer trước, sau đó fallback message
    cached = cache_get(query)
    return {
        "answer": cached or FALLBACK_ANSWER,
        "model": "fallback-cache" if cached else "fallback",
        "degraded": True,
    }


@app.post("/api/v1/chat")
async def chat(request: ChatRequest):
    start = time.time()
    result = await run_chat_pipeline(request.query, request.embedding)
    result["latency_ms"] = round((time.time() - start) * 1000, 2)
    return result


@app.get("/health")
def health():
    return {"status": "ok"}
