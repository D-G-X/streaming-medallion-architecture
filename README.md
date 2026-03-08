# Streaming Medallion Architecture

> An end-to-end, real-time data streaming and processing pipeline demonstrating the **Medallion Architecture** (Bronze, Silver, Gold). Built to handle continuous data flows, it ingests live cryptocurrency market data, streams it through a message broker, transforms it into a relational schema, and calculates real-time business aggregations.


## Architecture & Tech Stack

* **Data Source:** CoinGecko Public API (Live BTC & ETH prices)
* **Message Broker:** Redpanda (Lightweight, Kafka-compatible)
* **Processing & Orchestration:** Python, FastAPI, Docker Compose
* **Storage & Transformations:** PostgreSQL 15, SQLAlchemy ORM

---

## Pipeline Stages

### Bronze Layer (Raw Ingestion)
The producer (`producer/main.py`) runs an asynchronous FastAPI background task that fetches live data from the CoinGecko API. 
* **Engineering Detail:** The producer utilizes `asyncio.sleep()` to purposely throttle ingestion to 6 requests/minute, respecting the public API rate limits while ensuring a continuous, error-free stream. Raw JSON payloads are pushed to the `raw-market-data` Redpanda topic.

### Silver Layer (Cleansed & Conformed)
The consumer (`consumer/main.py`) subscribes to the Redpanda topic, acting as an infinite stream processor.
* **Engineering Detail:** It deserializes the raw JSON, flattens the nested data structures, enforces strict data types, and uses SQLAlchemy to load individual, timestamped records into the `silver_market_data` PostgreSQL table. It includes graceful shutdown handling for safe database connection closures.

### Gold Layer (Business Aggregations)
The presentation layer is handled natively within PostgreSQL for maximum query performance.
* **Engineering Detail:** A Python setup script (`gold/setup_views.py`) programmatically executes DDL to create the `crypto_market_metrics` view. This view leverages SQL's `date_trunc` to calculate real-time, 1-minute tumbling window metrics (moving average, high, low, and data point counts) directly from the Silver table.

---

## 🚀 Quick Start Guide

Follow these steps to clone the repository, set up your environment, and run the complete Medallion Architecture pipeline locally.

### Prerequisites
* **Docker & Docker Compose** installed and running
* **Python 3.9+**
* **Git**

---

### 1. Clone the Repository & Start Infrastructure
First, fetch the code and spin up the required database and message broker containers.

```bash
# Clone the repository
git clone [https://github.com/your-username/your-repo-name.git](https://github.com/your-username/your-repo-name.git)
cd your-repo-name

# Start Redpanda and PostgreSQL in the background
docker-compose up -d
```
