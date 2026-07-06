# Lab #28 — Full Platform Integration Sprint

AI platform với kiến trúc hybrid (Local + Kaggle GPU) sử dụng Prefect, Kafka, Qdrant, Prometheus, Grafana.

## Kiến trúc

```
Local (Docker Compose):
  Kafka → Prefect → Delta Lake → Feast (Redis)
  ↓                ↓
  Qdrant         API Gateway (FastAPI)
  ↓                ↓
  Prometheus ← Grafana
  ↓
  LangSmith tracing

Kaggle (GPU T4/P100):
  vLLM serving
  Embedding service
  MLflow tracking
```

## Yêu cầu

- Docker Desktop đang chạy
- Python 3.10+
- Tài khoản Kaggle với GPU đã bật
- **Tunnel service** (chọn 1 trong 2):
  - `ngrok` đã cài và token configured
  - HOẶC `cloudflared` đã cài (`brew install cloudflare/cloudflare/cloudflared`)

## Quick Start

### 1. Khởi động Local Stack

```bash
cd lab28
docker compose up -d
docker compose ps  # Kiểm tra tất cả services Up
```

**Services:**
- Prefect UI: http://localhost:4200
- Grafana: http://localhost:3000 (admin/admin)
- Qdrant: http://localhost:6333/dashboard
- Prometheus: http://localhost:9090
- API Gateway: http://localhost:8000

### 2. Setup Kaggle GPU

Tạo Kaggle Notebook với GPU T4 x2, chọn 1 trong 2 option:

**Option A: Single GPU (đơn giản - dùng 1 GPU)**

```python
# Cell 1: Install dependencies
!pip install -q vllm fastapi uvicorn pyngrok mlflow sentence-transformers

# Nếu cài vLLM bị lỗi, dùng fallback:
# !pip install transformers==4.46.3 --quiet
# !pip install vllm==0.7.3 --quiet

# Cell 2: Setup ngrok
from pyngrok import ngrok
ngrok.set_auth_token("YOUR_NGROK_TOKEN")  # lấy tại ngrok.com

# Cell 3: Start vLLM server (single GPU)
import subprocess, threading, time

def run_vllm():
    subprocess.run([
        "python", "-m", "vllm.entrypoints.openai.api_server",
        "--model", "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
        "--port", "8001",
        "--max-model-len", "4096",
        "--gpu-memory-utilization", "0.5"
    ])

thread = threading.Thread(target=run_vllm, daemon=True)
thread.start()
time.sleep(60)
print("vLLM server started")

# Cell 4: Create ngrok tunnel
tunnel = ngrok.connect(8001, "http")
print(f"vLLM URL: {tunnel.public_url}")
```

**Option B: Multi-GPU (nâng cao - dùng 2 GPUs)**

```python
# Cell 1: Install dependencies
!pip install -q vllm fastapi uvicorn pyngrok mlflow sentence-transformers

# Nếu cài vLLM bị lỗi, dùng fallback:
# !pip install transformers==4.46.3 --quiet
# !pip install vllm==0.7.3 --quiet

# Cell 2: Setup ngrok
from pyngrok import ngrok
ngrok.set_auth_token("YOUR_NGROK_TOKEN")  # lấy tại ngrok.com

# Cell 3: Start vLLM server (multi-GPU)
import subprocess
import os
import time
import requests
import threading

MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4"

def start_server(gpu_id, port):
    env = os.environ.copy()
    env["CUDA_VISIBLE_DEVICES"] = str(gpu_id)

    proc = subprocess.Popen(
        [
            "vllm", "serve", MODEL_NAME,
            "--dtype", "float16",
            "--max-model-len", "8192",
            "--host", "0.0.0.0",
            "--port", str(port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env
    )

    def stream_logs():
        for line in proc.stdout:
            print(f"[GPU {gpu_id}] {line.decode()}", end="")

    threading.Thread(target=stream_logs, daemon=True).start()

    return proc

print("Starting Server on GPU 0 (Port 8000)")
proc1 = start_server(0, 8000)

print("Starting Server on GPU 1 (Port 8001)")
proc2 = start_server(1, 8001)

def wait_for_server(port):
    print(f" Waiting for server on port {port}...")
    for _ in range(60):
        try:
            r = requests.get(f"http://localhost:{port}/health")
            if r.status_code == 200:
                print(f"Server on port {port} is ready!")
                return
        except:
            time.sleep(5)
    raise RuntimeError(f"Server on port {port} failed to start.")

wait_for_server(8000)
wait_for_server(8001)

# Cell 4: Create ngrok tunnel
print("Creating ngrok tunnels...")
tunnel1 = ngrok.connect(8000, "http")
tunnel2 = ngrok.connect(8001, "http")

print(f"GPU 0 URL: {tunnel1.public_url}")
print(f"GPU 1 URL: {tunnel2.public_url}")
# Có thể dùng 1 trong 2 hoặc cả 2 cho load balancing
```

**Option C: Dùng cloudflared (Single GPU)**

```python
# Cell 1: Install dependencies
!pip install -q vllm fastapi uvicorn cloudflared mlflow sentence-transformers

# Nếu cài vLLM bị lỗi, dùng fallback:
# !pip install transformers==4.46.3 --quiet
# !pip install vllm==0.7.3 --quiet

# Cell 2: Start vLLM server (single GPU)
import subprocess, threading, time

def run_vllm():
    subprocess.run([
        "python", "-m", "vllm.entrypoints.openai.api_server",
        "--model", "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
        "--port", "8001",
        "--max-model-len", "4096",
        "--gpu-memory-utilization", "0.5"
    ])

thread = threading.Thread(target=run_vllm, daemon=True)
thread.start()
time.sleep(60)
print("vLLM server started")

# Cell 3: Create cloudflare tunnel
import subprocess
tunnel = subprocess.run(["cloudflared", "tunnel", "--url", "http://localhost:8001"], capture_output=True, text=True)
print(tunnel.stdout)  # URL sẽ hiển thị
```

**Option D: Dùng cloudflared (Multi-GPU)**

```python
# Cell 1: Install dependencies
!pip install -q vllm fastapi uvicorn cloudflared mlflow sentence-transformers

# Nếu cài vLLM bị lỗi, dùng fallback:
# !pip install transformers==4.46.3 --quiet
# !pip install vllm==0.7.3 --quiet

# Cell 2: Start vLLM server (multi-GPU)
import subprocess
import os
import time
import requests
import threading

MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4"

def start_server(gpu_id, port):
    env = os.environ.copy()
    env["CUDA_VISIBLE_DEVICES"] = str(gpu_id)

    proc = subprocess.Popen(
        [
            "vllm", "serve", MODEL_NAME,
            "--dtype", "float16",
            "--max-model-len", "8192",
            "--host", "0.0.0.0",
            "--port", str(port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env
    )

    def stream_logs():
        for line in proc.stdout:
            print(f"[GPU {gpu_id}] {line.decode()}", end="")

    threading.Thread(target=stream_logs, daemon=True).start()

    return proc

print("Starting Server on GPU 0 (Port 8000)")
proc1 = start_server(0, 8000)

print("Starting Server on GPU 1 (Port 8001)")
proc2 = start_server(1, 8001)

def wait_for_server(port):
    print(f" Waiting for server on port {port}...")
    for _ in range(60):
        try:
            r = requests.get(f"http://localhost:{port}/health")
            if r.status_code == 200:
                print(f"Server on port {port} is ready!")
                return
        except:
            time.sleep(5)
    raise RuntimeError(f"Server on port {port} failed to start.")

wait_for_server(8000)
wait_for_server(8001)

# Cell 3: Create cloudflare tunnel
import subprocess
print("Creating cloudflare tunnels...")
tunnel1 = subprocess.run(["cloudflared", "tunnel", "--url", "http://localhost:8000"], capture_output=True, text=True)
tunnel2 = subprocess.run(["cloudflared", "tunnel", "--url", "http://localhost:8001"], capture_output=True, text=True)
print(f"GPU 0 URL (copy from output):")
print(tunnel1.stdout)
print(f"GPU 1 URL (copy from output):")
print(tunnel2.stdout)
# Có thể dùng 1 trong 2 hoặc cả 2 cho load balancing
```

### 3. Cập nhật Environment Variables

```bash
# Copy và chỉnh sửa file .env
cp .env.example .env
# Thay VLLM_NGROK_URL với URL từ Kaggle (ngrok hoặc cloudflared)
# Thay EMBED_NGROK_URL nếu có embedding service
# Thay LANGCHAIN_API_KEY với key của bạn
```

> **Fallback mode:** nếu chưa có Kaggle tunnel, platform vẫn chạy được end-to-end.
> API Gateway trả về cached/fallback answer (flag `degraded: true`), và script embed
> dùng local pseudo-embedding. Khi có URL thật trong `.env`, chạy
> `docker compose up -d api-gateway` để gateway nhận config mới.

Cài dependencies cho scripts và tests trên máy host:

```bash
pip install -r requirements.txt
```

### 4. Deploy Prefect Flows

Flow `kafka-to-delta` được **tự động deploy + serve** (schedule 5 phút/lần) bởi
service `prefect-flow-serve` trong docker-compose — không cần thao tác thêm.
Kiểm tra tại Prefect UI: http://localhost:4200/deployments

Chạy flow thủ công khi cần:

```bash
# Trigger 1 run ngay qua deployment:
docker compose exec prefect-flow-serve prefect deployment run 'Kafka to Delta Pipeline/kafka-to-delta'

# HOẶC chạy trực tiếp 1 lần (không qua scheduler):
docker compose exec prefect-flow-serve python /opt/prefect/flows/kafka_to_delta.py run

# HOẶC từ máy host (cần Python 3.10-3.12 + pip install -r prefect/flows/requirements.txt):
cd prefect/flows && python kafka_to_delta.py run
```

### 5. Ingest Data vào Kafka

```bash
cd ../..
python scripts/01_ingest_to_kafka.py     # Integration 1: Data → Kafka
python scripts/05_embed_to_qdrant.py     # Integration 5: Embeddings → Qdrant
python scripts/03_delta_to_feast.py      # Integration 3+4: Delta → Feast (chạy sau khi flow đã consume)
```

### 6. Chạy Smoke Tests

```bash
pytest smoke-tests/ -v
```

Kỳ vọng: 5/5 tests passing

### 7. Production Readiness Check

```bash
python scripts/production_readiness_check.py
```

Kỳ vọng: Score >80%

## Scripts

| Script | Mô tả |
|--------|-------|
| `scripts/01_ingest_to_kafka.py` | Ingest sample data vào Kafka |
| `scripts/03_delta_to_feast.py` | Load từ Delta Lake và push features vào Feast (Redis) |
| `scripts/05_embed_to_qdrant.py` | Embed data và lưu vectors vào Qdrant |
| `scripts/09_verify_observability.py` | Kiểm tra Prometheus metrics và LangSmith traces |
| `scripts/production_readiness_check.py` | Production readiness checklist |

## API Gateway

**Health Check:**
```bash
curl http://localhost:8000/health
```

**Chat Endpoint:**
```bash
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is platform engineering?",
    "embedding": [0.1, 0.2, ...]
  }'
```

## Monitoring

- **Grafana Dashboard:** http://localhost:3000 (admin/admin) — dashboard "Lab28 — AI Platform API Gateway" được auto-provision với 4 panels: request rate, P95 latency, error rate, service up
- **Prometheus:** http://localhost:9090
- **Prefect UI:** http://localhost:4200
- **LangSmith traces:** https://smith.langchain.com — project `lab28-platform` (cần `LANGCHAIN_API_KEY` trong `.env`)

Tạo traffic để dashboard có data:

```bash
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:8000/api/v1/chat \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"load test $i\", \"embedding\": [0.1]}" > /dev/null &
done
wait
```

## Troubleshooting

**Services không start:**
```bash
docker compose logs <service_name>
docker compose down -v
docker compose up -d
```

**Prefect worker không connect:**
```bash
# Check Prefect UI: http://localhost:4200
# Đảm bảo worker và flow-serve đang chạy:
docker compose logs prefect-worker
docker compose logs prefect-flow-serve
```

**Kafka consumer lag:**
```bash
# Kiểm tra topic
docker exec lab28-kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

## Graceful Degradation Demo

```bash
# Kill Qdrant → gateway vẫn trả lời (không có RAG context)
docker compose stop qdrant
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "test degradation", "embedding": [0.1]}'
docker compose start qdrant
```

## Nộp Bài

- Xem `SUBMISSION.md` ở thư mục gốc project.
- Trả lời 5 câu hỏi nộp bài: xem `ANSWERS.md`.
