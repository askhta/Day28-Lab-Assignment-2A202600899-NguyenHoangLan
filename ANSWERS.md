# Trả Lời 5 Câu Hỏi Nộp Bài — Lab #28

**Sinh viên:** Nguyễn Hoàng Lan — 2A202600899

---

## 1. Phân tích các trade-offs trong thiết kế kiến trúc AI platform. Cân bằng giữa performance, reliability, và maintainability như thế nào?

**Trade-off chính: hybrid Local + Kaggle.** GPU inference (vLLM) được tách lên Kaggle để tiết kiệm chi phí (GPU T4 miễn phí) trong khi toàn bộ data/orchestration/monitoring chạy local bằng Docker Compose. Đổi lại, ta chấp nhận:

- **Performance:** mỗi inference request phải đi qua tunnel ngrok/cloudflared (thêm ~100–300ms network latency). Bù lại, embedding + LLM chạy trên GPU nhanh hơn nhiều so với CPU local, nên tổng thể vẫn lợi. Vector search giữ ở local (Qdrant) để tránh round-trip không cần thiết.
- **Reliability:** tunnel là single point of failure. Chúng tôi bù bằng graceful degradation ở API Gateway: timeout rõ ràng (30s cho LLM, 5s cho Qdrant), fallback sang cached response trong Redis khi Kaggle mất kết nối, và context rỗng khi Qdrant down thay vì fail cả request.
- **Maintainability:** mỗi service là 1 container độc lập, config tập trung trong `docker-compose.yml` + `.env` (12-factor). Mỗi integration point là 1 script riêng có thể chạy/test độc lập, nên debug từng khâu rất dễ. Đánh đổi là nhiều moving parts hơn so với monolith — được giảm nhẹ bằng smoke tests và production readiness check tự động.

**Trade-off thứ hai: Kafka ở giữa ingestion và processing.** Thêm 1 hop và độ phức tạp vận hành (Zookeeper), nhưng đổi lấy decoupling, buffering khi downstream chậm, và khả năng replay (xem câu 3).

**Trade-off thứ ba: giả lập Delta Lake bằng Parquet và Feast bằng Redis.** Đơn giản hoá để chạy được trên laptop trong 2 giờ, giữ đúng *interface pattern* (batch layer → online feature store) để sau này swap sang Delta/Feast thật mà không đổi kiến trúc.

## 2. Trong kiến trúc hybrid (Local + Kaggle), xử lý ngắt kết nối giữa local và Kaggle như thế nào? Có cơ chế fallback không?

Có, fallback được implement ở `api-gateway/main.py` theo 3 lớp:

1. **Timeout chặt:** mọi call sang Kaggle (vLLM 30s, embedding 10s) đều có timeout, không bao giờ treo request vô hạn.
2. **Cached responses (Redis):** mỗi câu trả lời thành công từ vLLM được cache vào Redis (`answer_cache:{query}`, TTL 1 giờ). Khi tunnel đứt, request trùng query sẽ nhận cached answer với flag `"degraded": true, "model": "fallback-cache"`.
3. **Fallback message:** nếu không có cache, gateway trả về message rõ ràng báo backend unreachable (HTTP 200, `degraded: true`) thay vì 500 — client biết chính xác trạng thái và có thể retry.

Tương tự, embedding service có fallback sang local hash-based embedding (`scripts/05_embed_to_qdrant.py`) để pipeline ingestion không bị chặn khi Kaggle offline. Việc phát hiện đứt kết nối cũng hiện trên Grafana (error rate + latency panel) để operator biết cần restart tunnel.

## 3. Event-driven architecture với Kafka giúp decouple các components như thế nào?

- **Producer không biết consumer:** `scripts/01_ingest_to_kafka.py` chỉ ghi vào topic `data.raw`, không cần biết Prefect flow, Delta Lake hay Feast tồn tại. Có thể thêm consumer mới (ví dụ realtime embedding indexer) mà không sửa producer.
- **Temporal decoupling:** nếu Prefect worker đang restart, messages nằm lại trong Kafka; khi worker lên lại, flow consume tiếp từ offset đã commit (group `delta-writer`) — không mất data. Ngược lại nếu gọi trực tiếp (HTTP), producer sẽ fail khi consumer down.
- **Replay:** vì Kafka giữ log, có thể reset offset về `earliest` để rebuild toàn bộ Delta Lake / feature store sau khi sửa bug trong pipeline — điều không thể làm với point-to-point call.
- **Backpressure/buffering:** khi ingestion burst nhanh hơn tốc độ ghi Parquet, Kafka hấp thụ phần chênh lệch; pipeline batch xử lý theo nhịp riêng (schedule 5 phút/lần của Prefect).

## 4. Observability được implement như thế nào? Logs, metrics, traces thu thập và visualize ra sao?

- **Metrics:** API Gateway dùng `prometheus-fastapi-instrumentator` expose `/metrics` (request count, duration histogram theo handler/status). Prometheus scrape mỗi 15s (`monitoring/prometheus.yml`). Grafana được **auto-provision** (datasource + dashboard `lab28.json`) với 4 panels: request rate, P95 latency, 5xx error rate, và service up/down.
- **Logs:** tất cả services log ra stdout, xem tập trung bằng `docker compose logs -f <service>`. Gateway log warning có prefix `[WARN]` cho từng loại degradation (Qdrant down, vLLM unreachable) nên đọc log là biết ngay khâu nào đứt.
- **Traces:** Integration 10 dùng LangSmith — pipeline `chat` được wrap bằng `@traceable(name="chat-pipeline")`, mỗi request tạo 1 run trong project `lab28-platform` (bật khi có `LANGCHAIN_API_KEY`). Verify bằng `scripts/09_verify_observability.py`.
- **Workflow observability:** Prefect UI (localhost:4200) hiển thị trạng thái từng flow run, retry, và logs của pipeline Kafka→Delta.

## 5. Nếu một service trong stack (Qdrant hoặc Kafka) crash, hệ thống xử lý như thế nào? Có graceful degradation không?

Có, từng dependency được isolate bằng try/except + timeout riêng trong gateway:

- **Qdrant crash:** vector search fail → gateway log warning và tiếp tục gọi LLM **không có context** (RAG tạm tắt). Request vẫn trả 200, chất lượng answer giảm nhưng service không chết. Demo được bằng `docker compose stop qdrant` rồi gọi `/api/v1/chat`.
- **Kafka crash:** chỉ ảnh hưởng đường ingestion (async), không ảnh hưởng đường serving — user vẫn chat bình thường. Producer script sẽ báo lỗi connect; khi Kafka lên lại, Prefect flow consume tiếp từ offset cũ, không mất message đã ghi trước đó (persistence trong Kafka log).
- **Redis crash:** gateway coi cache là optional (`_cache = None` khi connect fail) — mất fallback cache nhưng đường chính vẫn chạy.
- **vLLM/Kaggle crash:** xem câu 2 (cached answer → fallback message).
- **Phát hiện:** `/health` endpoint + Prometheus `up` metric + Grafana error-rate panel cho biết service nào down; `scripts/production_readiness_check.py` chạy toàn bộ checklist trong 1 lệnh.

Nguyên tắc chung: **fail one leg, not the whole body** — mỗi integration point degrade độc lập, không có exception nào được phép lan lên thành 500 cho user.
