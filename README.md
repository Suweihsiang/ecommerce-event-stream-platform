# 🚀 電商事件流 Lakehouse 平台

使用 Kafka、Spark Structured Streaming、Iceberg、MinIO 與 Airflow 建立完整的電商事件流資料平台。

本專案模擬電商網站的使用者行為事件（瀏覽、點擊、加入購物車、購買），並實作從資料收集、資料治理、資料修復、資料監控到商業分析的完整資料工程流程。

---

# 🎯 專案目標

本專案旨在實作一套接近真實生產環境的 Event Stream Data Platform，涵蓋：

* 即時事件流處理（Streaming）
* Lakehouse 架構
* 增量資料處理（Incremental Processing）
* 資料品質管理（Data Quality）
* 異常資料修復（Replay）
* Schema Evolution
* Metadata 管理
* 工作流程編排（Airflow）

---

# 🏗️ 系統架構

```text
Kafka
    ↓
Bronze Layer (Iceberg)
    ↓
Silver Layer
    ├─ Validation
    ├─ Deduplication
    ├─ Quarantine
    └─ Replay Engine
    ↓
Gold Layer
    ├─ Monitoring Tables
    └─ Analytics Tables
    ↓
Data Quality Metrics
    ↓
Telegram Alert
```

---

# 🛠️ 技術棧

## Streaming

* Apache Kafka

## Processing

* Apache Spark Structured Streaming
* PySpark

## Storage

* Apache Iceberg
* MinIO

## Orchestration

* Apache Airflow

## Monitoring

* Telegram Bot

---

# 📦 Spark Dependencies

本專案使用以下 Spark Packages：

```bash
--packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,org.apache.hadoop:hadoop-aws:3.3.4

---

# 📂 資料分層

## Bronze Layer（原始資料層）

負責儲存 Kafka 原始事件資料。

已實作：

* Kafka Ingestion
* Iceberg Storage
* Hidden Partition
* Incremental Snapshot Read
* Schema Evolution

---

## Silver Layer（資料治理層）

負責資料驗證、清洗與修復。

已實作：

* Validation
* Deduplication
* Quarantine
* Multiple Error Detection
* Replay Engine
* Repair Rules

---

## Gold Layer（分析層）

負責提供營運分析與商業指標。

### Monitoring Tables

* event_type_5min_count
* category_5min_count
* revenue_5min
* active_user_5min

### Analytics Tables

* device_5min_summary
* category_conversion_5min
* user_activity_5min_summary
* user_profile

---

# 🔄 Replay Engine

Replay Engine 負責自動修復可恢復的異常資料。

目前支援：

| 錯誤類型               | 修復方式               |
| ------------------ | ------------------ |
| invalid_event_type | 修正為 view           |
| price_is_negative  | 取絕對值               |
| event_time_is_null | 使用 kafka_timestamp |

修復流程：

```text
bad_events
    ↓
repair_rules
    ↓
Replay Engine
    ↓
bronze_clean
    ↓
pipeline_partitions
```

---

# 📊 Metadata 與監控

Metadata Tables：

* pipeline_state
* pipeline_partitions
* pipeline_quality_summary
* replay_quality_summary
* schema_drift_summary
* schema_change_requests
* data_quality_metrics
* repair_rules

監控功能：

* Data Quality Metrics
* Schema Drift Detection
* Telegram Alert

---

# ⚙️ Airflow DAG

## 主流程

```text
bronze_to_silver
        ↓
silver_to_gold
        ↓
check_data_quality
```

排程：

* 每小時執行

---

## Replay 流程

```text
replay_bad_events
```

執行方式：

* 手動 Trigger
* 或低頻排程

---

# ✨ 專案特色

✅ Kafka 即時事件流

✅ Spark Structured Streaming

✅ Iceberg Lakehouse

✅ Incremental Processing

✅ Schema Evolution

✅ Data Validation

✅ Deduplication

✅ Quarantine

✅ Replay Engine

✅ Repair Rules

✅ Data Quality Metrics

✅ Telegram Alert

✅ Airflow Orchestration

✅ Customer 360（user_profile）

---

# 📌 專案狀態

本專案已完成從資料收集、資料治理、資料修復、資料監控到商業分析的完整 Event Stream Lakehouse 流程。
