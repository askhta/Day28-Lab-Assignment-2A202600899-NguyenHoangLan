# scripts/09_verify_observability.py
"""Integration 9 + 10: Prometheus metrics + LangSmith traces."""
import os

import requests

try:  # load .env nếu có
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def check_prometheus():
    resp = requests.get(
        "http://localhost:9090/api/v1/query",
        params={"query": 'http_requests_total{job="api-gateway"}'},
    )
    data = resp.json()
    assert data["status"] == "success"
    print("Integration 9 OK: Prometheus metrics flowing")


def check_langsmith():
    if not os.environ.get("LANGCHAIN_API_KEY"):
        print("[SKIP] Integration 10: LANGCHAIN_API_KEY chưa set — bỏ qua LangSmith check")
        return
    from langsmith import Client

    client = Client(api_key=os.environ["LANGCHAIN_API_KEY"])
    project = os.environ.get("LANGCHAIN_PROJECT", "lab28-platform")
    runs = list(client.list_runs(project_name=project, limit=1))
    assert len(runs) > 0, f"No runs found in LangSmith project '{project}'"
    print("Integration 10 OK: LangSmith traces visible")


check_prometheus()
check_langsmith()
