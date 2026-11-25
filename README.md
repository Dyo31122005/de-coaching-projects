# Data Engineering Coaching Lab: Projects

Các project cuối module trong khoá Data Engineering Coaching.

| Project | Mô tả | Công nghệ |
|---|---|---|
| [spark/99-project](spark/99-project) | Đọc log hành vi người dùng từ Kafka, làm giàu dữ liệu (quốc gia theo IP, browser, OS), lưu vào Postgres theo star schema, dashboard báo cáo | Spark Structured Streaming, Kafka, YARN, Postgres, Streamlit |
| [airflow/99-project](airflow/99-project) | Giám sát pipeline trên: health check Kafka (broker, topic, watermark, consumer lag) và Spark/YARN (master, worker CPU/RAM, dữ liệu output), cảnh báo qua Telegram | Airflow 2.10 (custom operators, TaskFlow), Kafka AdminClient, YARN REST API, WebHDFS, Telegram Bot API |

```
Kafka (product_view) ──> Spark Structured Streaming (YARN) ──> Postgres (fact_page_view) ──> Streamlit
        │                          │ checkpoint (HDFS)                    │
        └──────────── Airflow: kafka_monitoring / spark_monitoring ───────┘ ──> Telegram
```

## Cấu hình

Mật khẩu, địa chỉ broker **không** nằm trong code, truyền qua biến môi trường (xem [.env.example](.env.example))
hoặc Airflow Connections (xem [airflow/99-project/README.md](airflow/99-project/README.md#3-tạo-airflow-connections)).

Hạ tầng (Hadoop/YARN, Kafka, Postgres, Airflow trên Docker) dùng môi trường của khoá học, không kèm trong repo này.

## Ghi nhận

- `spark/99-project/browser/` và đề bài các project thuộc tài liệu của khoá học.
- This product includes IP2Location LITE data available from
  [https://lite.ip2location.com](https://lite.ip2location.com) (`spark/99-project/data/IP2LOCATION-LITE-DB1.BIN`,
  license CC BY-SA 4.0).
