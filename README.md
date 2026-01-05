# 🔐 Real-Time Fraud Detection with Apache Flink & Kafka

A **near real-time fraud detection system** built using **Apache Flink (PyFlink)** and **Apache Kafka**, simulating high-volume financial transactions and detecting fraudulent behavior using **stateful stream processing** and **event-time semantics**.

---

## 🚀 Project Overview

This project demonstrates how real-world payment systems detect fraud **as transactions happen**, not after the fact.

The system:

* Ingests transaction events from Kafka
* Processes them using Flink’s **Keyed State**
* Applies multiple fraud detection rules
* Emits fraud alerts in near real-time

Designed to scale to **millions of events per second** using Flink’s distributed architecture.

---

## 🧠 Fraud Detection Rules Implemented

| Rule              | Description                                              |
| ----------------- | -------------------------------------------------------- |
| High Amount       | Flags unusually large transactions                       |
| Velocity          | Detects multiple transactions within a short time window |
| Impossible Travel | Detects geographically impossible card usage             |
| Risk Scoring      | Combines rules into a fraud score                        |

Each rule operates on **keyed state per card**, similar to real payment systems.

---

## ⚙️ Architecture

```
Kafka Producer (Synthetic Transactions)
        ↓
Kafka Topic (transactions)
        ↓
Apache Flink (Event-Time Processing)
        ↓
Stateful Fraud Rules (KeyedProcessFunction)
        ↓
FraUD / Legit Stream Outputs
```

---

## ⏱️ Real-Time Processing Features

✔ Event-time processing with watermarks
✔ Stateful processing using Keyed State
✔ Time-bounded state with TTL
✔ Near real-time fraud detection (seconds latency)
✔ Horizontal scalability via key partitioning
✔ Exactly-once semantics (checkpointing ready)

---

## 🗃️ State Management Strategy

* State is **keyed by `card_id`**
* Each card maintains only minimal state:

  * Recent transaction timestamps
  * Last known location
  * Last event time
* State automatically expires using **TTL**

This prevents unbounded state growth even at high throughput.

---

## 📦 Technology Stack

* **Apache Flink 2.x (PyFlink)**
* **Apache Kafka**
* **Docker & Docker Compose**
* **Python 3.10**
* **Event-Time & Watermarks**
* **RocksDB (recommended for production)**

---

## 🧪 Example Output

```
FRAUD | {
  "card_id": "card_3",
  "amount": 250.0,
  "location": "CA",
  "score": 80,
  "status": "FRAUD",
  "severity": "HIGH",
  "event_time": "2025-12-20T09:48:01Z"
}
```

## 📈 Scalability Considerations

* Supports millions of transactions per second via:

  * Keyed partitioning
  * Parallel TaskManagers
  * Incremental checkpoints
* CEP patterns can be layered **after rule-based filtering**
* Backpressure handled automatically by Flink

---

## 📁 Repository Structure

```
.
├── producer/
│   └── transaction_producer.py
├── flink/
│   └── fraud_detection.py
├── docker-compose.yml
├── README.md
```

---

## 🧠 Future Enhancements

* Add Flink CEP patterns for complex fraud sequences
* Sink alerts to Kafka / Elasticsearch
* Integrate ML-based scoring
* Metrics & monitoring via Prometheus
* Exactly-once sinks
