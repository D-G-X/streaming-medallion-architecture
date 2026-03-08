# Streaming Medallion Architecture

> An end-to-end real-time data streaming and processing pipeline demonstrating the **Medallion Architecture** (Bronze, Silver, Gold). Built to handle continuous data flows, it ingests live cryptocurrency market data, streams it through a message broker, and processes it into a data warehouse for downstream analytics.


* **Data Source:** CoinGecko Public API (Live BTC & ETH prices)
* **Producer (Bronze Layer):** Python, FastAPI, `httpx`
* **Message Broker:** Redpanda (Lightweight, Kafka-compatible)
* **Containerization & Orchestration:** Docker Compose
* **Storage (Gold Layer):** PostgreSQL 15 *(Setup complete, integration pending)*

## Current Project Status: Bronze Layer Complete
The **Bronze Layer** (raw data ingestion) is fully operational. It reliably fetches data using asynchronous Python and streams it into the Redpanda broker without hitting API rate limits.

### Engineering Decisions
* **Redpanda over Kafka:** Chosen for its single-binary architecture, eliminating the need for Zookeeper while maintaining 100% Kafka API compatibility.
* **Rate Limiting:** The public CoinGecko API strictly limits requests. The FastAPI background task utilizes `asyncio.sleep(300)` to purposefully throttle ingestion to 6 requests/minute, ensuring continuous, error-free streaming without requiring paid API keys.

---

## How to Run locally (Docker)

### 1. Start the Infrastructure
Ensure your Docker daemon is running.
