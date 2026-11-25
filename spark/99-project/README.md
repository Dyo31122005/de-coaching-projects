# Yêu cầu dự án

## Overview

Bài toán kết hợp việc sử dụng `kafka` và `spark`. Sử dụng `spark` đọc dữ liệu từ `kafka` sau đó xử lý, tính toán và lưu
trữ vào db `postgres`

## Bài toán

**Đầu vào:**

- Kafka: Cụm `Kafka` setup ở dưới local và `topic` chứa dữ liệu về hành vi người dùng trên website đã làm trong project
  của module `Kafka`
- Spark: Cụm `Spark` cài đặt dưới local trong khóa học
- Schema của dữ liệu

**Đầu ra:**

- Thiết kế db. Sử dụng công cụ [dbdiagram.io](https://dbdiagram.io/)
- Chương trình code xử lý yêu cầu của dự án
- Kết quả của các báo cáo theo yêu cầu
- Dữ liệu được lưu trong database `Postgres`
- Đoạn code sử dụng thư viện [streamlit.io](https://streamlit.io/) để visualize báo cáo

## Mô tả

**Schema của dữ liệu đầu vào:**

| Tên          | Kiểu dữ liệu  | Mô tả                                                   | Ví dụ                                                                                                                                                               |
|--------------|---------------|---------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id           | String        | Log id                                                  | aea4b823-c5c6-485e-8b3b-6182a7c4ecce                                                                                                                                |
| api_version  | String        | Version của api                                         | 1.0                                                                                                                                                                 | 
| collection   | String        | Loại log                                                | view_product_detail                                                                                                                                                 | 
| current_url  | String        | Url của trang web mà người dùng đang vào                | https://www.glamira.cl/glamira-anillo-saphira-skug100335.html?alloy=white-375&diamond=sapphire&stone2=diamond-Brillant&itm_source=recommendation&itm_medium=sorting |
| device_id    | String        | id của thiết bị                                         | 874db849-68a6-4e99-bcac-fb6334d0ec80                                                                                                                                |
| email        | String        | Email của người dùng                                    |                                                                                                                                                                     |
| ip           | String        | Địa chỉ ip                                              | 190.163.166.122                                                                                                                                                     |
| local_time   | String        | Thời gian log được tạo. Format dạng yyyy-MM-dd HH:mm:ss | 2024-05-28 08:31:22                                                                                                                                                 |
| option       | Array<Object> | Danh sách các option của sản phẩm                       | `[{"option_id": "328026", "option_label": "diamond"}]`                                                                                                              |
| product_id   | String        | Mã id của sản phẩm                                      | 96672                                                                                                                                                               |
| referrer_url | String        | Đường dẫn web dẫn đến link `current_url`                | https://www.google.com/                                                                                                                                             |
| store_id     | String        | Mã id của cửa hàng                                      | 85                                                                                                                                                                  |
| time_stamp   | Long          | Timestamp thời điểm bản ghi log được tạo                |                                                                                                                                                                     |
| user_agent   | String        | Thông tin của browser, thiết bị                         | Mozilla/5.0 (iPhone; CPU iPhone OS 13_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1                           |

**Yêu cầu:**

Thiết kế db và viết chương trình đưa ra các báo cáo như sau:

- Top 10 `product_id` có lượt view cao nhất trong ngày hiện tại
- Top 10 quốc gia có lượt view cao nhất trong ngày hiện tại (quốc gia được lấy dựa vào ip. File `ip2location.bin` đã có từ level 1)
- Top 5 `referrer_url` có lượt view cao nhất trong ngày hiện tại
- Với 1 quốc gia bất kỳ, lấy ra danh sách các `store_id` và lượt view tương ứng, sắp xếp theo lượt view
  giảm dần
- Dữ liệu view phân bổ theo giờ của một `product_id` bất kỳ trong ngày
- Dữ liệu view theo giờ của từng `browser`, `os`

## Cách chạy

**Biến môi trường:** địa chỉ broker Kafka và mật khẩu không nằm trong code. Copy `.env.example` (ở thư mục gốc
repo) thành `.env`, điền giá trị, rồi thêm `--env-file <đường dẫn>/.env` vào các lệnh `docker run` bên dưới
(và `export $(cat .env | xargs)` trước khi chạy Streamlit).

**Nguồn dữ liệu:** topic Kafka `product_view` trên broker chung của lớp (không cần dựng Kafka local), schema thực tế dữ liệu có thể khác README một chút tuỳ loại `collection` (ví dụ field `option` là object hoặc array tuỳ loại log).

**Tra quốc gia theo IP:** dùng file [data/IP2LOCATION-LITE-DB1.BIN](data/IP2LOCATION-LITE-DB1.BIN) + package `IP2Location`. Khi chạy `--master yarn`, phải thêm `--files /spark/99-project/data/IP2LOCATION-LITE-DB1.BIN` để executor trên các node khác có file này (xem `country/country.py`, fallback đọc từ `data/` chỉ dùng được khi chạy local).

### 1. Khởi động hạ tầng

```shell
cd hadoop/00-setup/hadoop && docker compose up -d
cd ../../../postgres/00-setup/postgres && docker compose up -d
```

Postgres expose ở host port `5433` (port `5432` đã bị PostgreSQL native trên máy chiếm).

### 2. Tạo schema Postgres

```shell
docker run --rm --network streaming-network -e PGPASSWORD="$POSTGRES_PASSWORD" \
  -v ./spark/99-project/sql:/sql \
  postgres:16.3 psql -h postgres -U postgres -d postgres -f /sql/schema.sql
```

Xem [schema.dbml](sql/schema.dbml) trên [dbdiagram.io](https://dbdiagram.io/d) để coi sơ đồ. Thiết kế dạng **star schema**: 1 bảng fact (`fact_page_view`) + 5 bảng dimension (`dim_date`, `dim_location`, `dim_product`, `dim_store`, `dim_customer`).

`dim_location.region_name`/`city_name` hiện đang **NULL** vì file `IP2LOCATION-LITE-DB1.BIN` chỉ có dữ liệu country. Để có region/city thật: tải `IP2LOCATION-LITE-DB5.BIN` (cần tài khoản free tại [lite.ip2location.com](https://lite.ip2location.com)) và thay vào `data/IP2LOCATION-LITE-DB1.BIN` (đổi `_DB_FILENAME` trong [country/country.py](country/country.py) nếu đổi tên file) — không cần sửa logic khác, `region`/`city` sẽ tự có giá trị thật.

### 3. Chạy pipeline streaming (Kafka -> parse/enrich -> Postgres)

**Test nhanh (local mode, không cần conda-pack):**

```shell
docker run --rm -ti --name project99 \
--network=streaming-network \
-v ./spark/99-project:/spark/99-project \
-v spark_lib:/home/spark/.ivy2 \
-e CHECKPOINT_LOCATION=/tmp/99-project-checkpoint \
unigap/spark:3.5 bash -c "
pip install -q user-agents ua-parser IP2Location psycopg2-binary &&
cd /spark/99-project &&
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
--master 'local[2]' \
page_view_streaming.py"
```

(`CHECKPOINT_LOCATION` bắt buộc ở local mode vì không có `HADOOP_CONF_DIR` — xem ghi chú checkpoint bên dưới.)

**Chạy thật trên cụm YARN** (đã verify end-to-end, chạy từ thư mục `spark`):

```shell
docker container stop project99 || true &&
docker container rm project99 || true &&
docker run --rm -ti --name project99 \
--network=streaming-network \
-v ./:/spark \
-v spark_lib:/home/spark/.ivy2 \
-v spark_data:/data \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "
(cd /spark/99-project && zip -r /tmp/project99_libs.zip browser/* country/* device_os/* util/*) &&
conda env create --file /spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/project99_libs.zip \
--files /spark/99-project/data/IP2LOCATION-LITE-DB1.BIN \
--deploy-mode client \
--master yarn \
/spark/99-project/page_view_streaming.py"
```

Lưu ý: `conda pack` phải chạy ở working dir mặc định của container (không `cd` vào `/spark/99-project` trước đó) vì đây là volume mount khác filesystem — `conda pack` dùng `os.rename` nội bộ nên bị lỗi `Invalid cross-device link`/`Permission denied` nếu output nằm trên volume mount.

`checkpointLocation` đọc từ biến môi trường `CHECKPOINT_LOCATION` (mặc định `/user/spark/checkpoint/99-project`, xem [page_view_streaming.py](page_view_streaming.py)). Trên YARN, path không scheme sẽ tự resolve thành HDFS (vì `fs.defaultFS=hdfs://namenode` trong `hadoop-conf/core-site.xml`) nên phải nằm dưới `/user/spark/` (root HDFS chỉ `root:supergroup` có quyền write); khi chạy local mode không có `HADOOP_CONF_DIR`, path lại resolve thành local filesystem nên cần override bằng `CHECKPOINT_LOCATION=/tmp/...` (container không có quyền ghi `/user`).

### 4. Chạy dashboard Streamlit

```shell
cd spark/99-project/app
pip install -r requirements.txt
POSTGRES_HOST=localhost POSTGRES_PORT=5433 streamlit run app.py
```

Truy cập [http://localhost:8501](http://localhost:8501).

## Phụ lục

**Cách chạy chương trình sử dụng thư viện ngoài thông qua conda**

```
docker container stop test-spark || true &&
docker container rm test-spark || true &&
docker run --rm -ti --name test-spark \
--network=streaming-network \
-v ./:/spark \
-v spark_lib:/home/spark/.ivy2 \
-v spark_data:/data \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "(cd /spark/99-project && zip -r /tmp/browser.zip browser/*) &&
conda env create --file /spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/browser.zip \
--deploy-mode client \
--master yarn \
/spark/99-project/test.py"
```

## Link tham khảo

[Python Package Management](https://spark.apache.org/docs/latest/api/python/tutorial/python_packaging.html)

[JDBC To Other Databases](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

IP2Location LITE data: [https://lite.ip2location.com](https://lite.ip2location.com) (CC BY-SA 4.0)
