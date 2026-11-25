from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param

from project.alert.telegram import send_telegram_message, telegram_failure_callback
from project.operators.kafka_health_check import KafkaHealthCheckOperator

KAFKA_CONN_ID = 'kafka_monitor'
TOPICS = ['product_view']
# Các consumer group bắt buộc phải active. Để trống => chỉ list ra để theo dõi
REQUIRED_CONSUMER_GROUPS = []
# Spark structured streaming không commit offset vào Kafka => đọc offset từ checkpoint trên HDFS
# (phải trùng với CHECKPOINT_LOCATION khi spark-submit job spark/99-project)
WEBHDFS_CONN_ID = 'hdfs_namenode'
SPARK_CHECKPOINTS = {
    'spark:Project99PageViewStreaming': '/user/spark/checkpoint/99-project-20261118',
}

SNAPSHOT_VARIABLE = 'kafka_monitoring_last_snapshot'
INTERVAL_MINUTES = 5

# Ngưỡng cảnh báo
MIN_MESSAGES_PER_INTERVAL = 1      # topic không có message mới trong 5 phút => producer có vấn đề
# Topic nhận ~600 msg/phút, Spark xử lý mỗi 30s => lag bình thường vài trăm.
# 10_000 ~ consumer chậm khoảng 17 phút
MAX_CONSUMER_LAG = 10_000          # tổng lag của 1 group trên 1 topic
MIN_PROCESSING_RATIO = 0.5         # consumer xử lý < 50% số message mới VÀ lag tăng => consumer không theo kịp
# 2 lần chạy cách nhau quá gần (vd: trigger tay ngay sau lần chạy theo lịch) => Spark (batch 30s) chưa kịp xử lý
# => không so sánh, giữ mốc cũ
MIN_COMPARE_SECONDS = 120


@dag(
    dag_id='kafka_monitoring',
    schedule=f'*/{INTERVAL_MINUTES} * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={'send_report': Param(False, type='boolean', description='Gửi báo cáo lên Telegram kể cả khi không có cảnh báo')},
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
        'on_failure_callback': telegram_failure_callback,
    },
    tags=['project', 'monitoring', 'kafka'],
)
def kafka_monitoring():
    health_check = KafkaHealthCheckOperator(
        task_id='health_check',
        kafka_conn_id=KAFKA_CONN_ID,
        topics=TOPICS,
        consumer_groups=REQUIRED_CONSUMER_GROUPS,
        webhdfs_conn_id=WEBHDFS_CONN_ID,
        spark_checkpoints=SPARK_CHECKPOINTS,
    )

    @task
    def check_data_flow(current):
        """So sánh offset lần chạy này với lần chạy trước (5 phút trước).

        - Throughput     = high watermark hiện tại - high watermark lần trước  (message được ghi vào topic)
        - Processing     = committed offset hiện tại - committed offset lần trước (message consumer đã xử lý)
        - Consumer lag   = high watermark - committed offset
        """
        previous = Variable.get(SNAPSHOT_VARIABLE, default_var=None, deserialize_json=True)

        issues, lines = [], []
        if not previous:
            Variable.set(SNAPSHOT_VARIABLE, current, serialize_json=True)
            return {'issues': issues, 'lines': ['Lần chạy đầu tiên: đã lưu mốc offset, lần sau mới so sánh được.']}

        elapsed = (datetime.fromisoformat(current['checked_at'])
                   - datetime.fromisoformat(previous['checked_at'])).total_seconds()
        if elapsed < MIN_COMPARE_SECONDS:
            return {'issues': issues, 'lines': [f"Chỉ cách lần trước {elapsed:.0f}s (< {MIN_COMPARE_SECONDS}s): "
                                                f"bỏ qua so sánh, giữ mốc lúc {previous['checked_at'][11:19]} UTC"]}

        Variable.set(SNAPSHOT_VARIABLE, current, serialize_json=True)
        lines.append(f"Khoảng so sánh: {elapsed / 60:.1f} phút "
                     f"({previous['checked_at'][11:19]} -> {current['checked_at'][11:19]} UTC)")

        produced_by_topic = {}
        for topic, partitions in current['watermarks'].items():
            prev_partitions = previous['watermarks'].get(topic, {})
            produced = 0
            for p, offsets in sorted(partitions.items()):
                prev_high = prev_partitions.get(p, {}).get('high')
                if prev_high is None or offsets['high'] < prev_high:  # partition mới / topic bị tạo lại
                    continue
                produced += offsets['high'] - prev_high
                lines.append(f"  {topic}[{p}] high {prev_high} -> {offsets['high']} (+{offsets['high'] - prev_high})")

            produced_by_topic[topic] = produced
            lines.append(f"Topic {topic}: +{produced} message, {produced / elapsed * 60:.1f} msg/phút")
            if produced < MIN_MESSAGES_PER_INTERVAL:
                issues.append(f"Topic {topic} không có message mới trong {elapsed / 60:.1f} phút")

        for group_id, group in current['groups'].items():
            prev_offsets = previous['groups'].get(group_id, {}).get('offsets', {})
            for topic, partitions in group['offsets'].items():
                lag = sum(o['lag'] for o in partitions.values())
                if topic not in prev_offsets:  # group mới xuất hiện => lần này chỉ lưu mốc
                    lines.append(f"Group {group_id} ({group['state']}) / {topic}: lag={lag} (lần đầu thấy group, "
                                 f"lần sau mới tính được tốc độ xử lý)")
                    if lag > MAX_CONSUMER_LAG:
                        issues.append(f"Group {group_id} lag {lag} trên {topic} (ngưỡng {MAX_CONSUMER_LAG})")
                    continue

                processed = sum(o['committed'] - prev_offsets.get(topic, {}).get(p, o)['committed']
                                for p, o in partitions.items())
                prev_lag = sum(o['lag'] for o in prev_offsets.get(topic, {}).values())
                produced = produced_by_topic.get(topic, 0)

                lines.append(f"Group {group_id} ({group['state']}, {group['members']} member) / {topic}: "
                             f"lag={lag}, xử lý +{processed} message ({processed / elapsed * 60:.1f} msg/phút)")
                if lag > MAX_CONSUMER_LAG:
                    issues.append(f"Group {group_id} lag {lag} trên {topic} (ngưỡng {MAX_CONSUMER_LAG})")
                if produced and processed < produced * MIN_PROCESSING_RATIO and lag > prev_lag:
                    issues.append(f"Group {group_id} chỉ xử lý {processed}/{produced} message mới trên {topic}, "
                                  f"lag tăng {prev_lag} -> {lag}")

        if not current['groups']:
            lines.append("Không có consumer group nào trên broker")

        return {'issues': issues, 'lines': lines}

    @task
    def send_alert(health, flow, params=None):
        issues = health['issues'] + flow['issues']
        if not issues and not params['send_report']:
            print("Không có cảnh báo")
            return

        header = f"⚠️ KAFKA ALERT ({len(issues)} vấn đề)" if issues else "✅ KAFKA REPORT"
        topics = ', '.join(f"{t} ({len(p)} partition)" for t, p in health['watermarks'].items())
        message = '\n'.join([
            header,
            *[f"- {issue}" for issue in issues],
            '',
            f"Brokers: {', '.join(health['brokers'])}",
            f"Topics: {topics}",
            f"Consumer groups: {len(health['groups'])}",
            *flow['lines'],
        ])
        print(message)
        send_telegram_message(message)

    flow = check_data_flow(health_check.output)
    send_alert(health_check.output, flow)


kafka_monitoring()
