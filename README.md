# Real-Time Threat Ingestion & SIEM Log Streaming Pipeline 🔒

![Python](https://shields.io) ![Apache Kafka](https://shields.io) ![MongoDB](https://shields.io) ![Docker](https://shields.io)
![SIEM Telemetry](https://shields.io) ![SOC Operations](https://shields.io) ![Pipeline Security](https://shields.io) ![Build Status](https://shields.io)


## 📌 Project Overview
This repository contains a production-ready, event-driven data engineering pipeline designed to ingest, sanitize, and validate live network telemetry logs in real time. Leveraging **Apache Kafka** deployed via **Docker** containers, the pipeline automates the tracking of infrastructure access logs, applies automated Security Operations Center (SOC) alert triage rules, and commits the structured security records directly into a **MongoDB document database cluster**.

The system dynamically separates everyday corporate access profiles from high-risk indicators of compromise (IoCs), automatically isolating threat activity such as distributed brute-force attacks and high-volume data exfiltration events.

## 🛠️ Architecture & Technical Stack
- **Streaming Orchestration:** Apache Kafka (KRaft mode via Docker Container Engine)
- **Log Management Warehouse:** MongoDB (NoSQL Document Store via Docker Container Engine)
- **Automation & Pipeline Logic:** Python 3.x
- **Core Drivers:** `kafka-python-ng`, `pymongo`, `json`

## 📊 Live Telemetry Pipeline Data Flow
1. **The Telemetry Producer (`real_producer.py`):** Simulates a high-throughput enterprise firewall feed, broadcasting continuous multi-server network access metrics (timestamps, source IPs, failed login thresholds, and payload velocities in MB).
2. **The Event Message Broker (Docker Kafka Cluster):** Manages real-time message stream routing across isolated network bridges on port `9092`.
3. **The SIEM Triage Consumer (`real_consumer.py`):** Intercepts the streaming payloads, safely standardizes fields to avoid payload keys mismatch, applies immediate incident response rule parameters, and logs the records natively to MongoDB on port `27017`.

## 🚨 Automated Detection Profiles
- **Brute-Force Vector Flag:** Triggered instantly if an endpoint records ≥ 5 sequential failed validation loops.
- **Data Exfiltration Vector Flag:** Triggered instantly if a telemetry stream session exceeds a payload transfer velocity of `400.0 MB`.

## 🚀 Deployment Instructions

### 1. Boot up the Infrastructure
Ensure Docker Desktop is running in the background. In your project root folder, execute:
```bash
docker compose up -d
```

### 2. Activate the SIEM Monitor
Open a terminal panel and launch the consumer pipeline:
```bash
python real_consumer.py
```

### 3. Initiate the Telemetry Broadcast
Open a split terminal window pane and fire up the firewall simulator:
```bash
python real_producer.py
```

## 🛑 Safe Shutdown Protocol
To stop the streaming network and release all bound system ports cleanly, run:
```bash
docker compose down
```
