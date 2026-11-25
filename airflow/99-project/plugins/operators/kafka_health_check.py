import json
from datetime import datetime, timezone

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from confluent_kafka import ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec


class KafkaHealthCheckOperator(BaseOperator):
    """Kiểm tra sức khỏe Kafka và chụp lại (snapshot) offset hiện tại.

    1. Broker connectivity: kết nối được tới broker không
    2. Topic availability: list toàn bộ topic, kiểm tra các topic cần giám sát có tồn tại,
       partition nào không có leader
    3. Partition offset: low/high watermark của từng partition
    4. Consumer group: list toàn bộ group, trạng thái (state), số member,
       offset đã commit => consumer đã đọc đến đâu, lag bao nhiêu
    5. Spark streaming consumer: Spark structured streaming KHÔNG commit offset vào Kafka (không hiện trong
       consumer group) mà lưu trong checkpoint trên HDFS => đọc checkpoint qua WebHDFS để tính lag

    - Lỗi nghiêm trọng (broker down, topic không tồn tại) => raise => task fail => on_failure_callback
    - Vấn đề nhẹ (group không Stable, partition không có leader) => ghi vào `issues`
    - Return snapshot (dict) => tự push vào XCom để task sau so sánh
    """

    template_fields = ('topics', 'consumer_groups')

    def __init__(self, kafka_conn_id, topics, consumer_groups=None, webhdfs_conn_id=None, spark_checkpoints=None,
                 timeout=15, **kwargs):
        super().__init__(**kwargs)
        self.kafka_conn_id = kafka_conn_id
        self.topics = topics
        # Các group bắt buộc phải tồn tại và đang Stable. None => chỉ list ra, không bắt buộc
        self.consumer_groups = consumer_groups or []
        # {"<tên consumer>": "<đường dẫn checkpoint trên HDFS>"}
        self.webhdfs_conn_id = webhdfs_conn_id
        self.spark_checkpoints = spark_checkpoints or {}
        self.timeout = timeout

    def execute(self, context):
        conn = BaseHook.get_connection(self.kafka_conn_id)
        admin = AdminClient(conn.extra_dejson)
        issues = []

        brokers, all_topics = self._check_brokers_and_topics(admin)
        watermarks = self._get_watermarks(admin, all_topics, issues)
        groups = self._check_consumer_groups(admin, watermarks, issues)
        groups.update(self._check_spark_checkpoints(watermarks, issues))

        return {
            'checked_at': datetime.now(timezone.utc).isoformat(),
            'brokers': brokers,
            'watermarks': watermarks,
            'groups': groups,
            'issues': issues,
        }

    # 1 + 2. Broker & topic ---------------------------------------------------
    def _check_brokers_and_topics(self, admin):
        try:
            metadata = admin.list_topics(timeout=self.timeout)
        except KafkaException as e:
            raise AirflowException(f"Không kết nối được Kafka broker: {e}")

        brokers = [f"{b.id}@{b.host}:{b.port}" for b in metadata.brokers.values()]
        if not brokers:
            raise AirflowException("Kafka không có broker nào available")
        self.log.info("Brokers (%d): %s", len(brokers), brokers)

        user_topics = sorted(t for t in metadata.topics if not t.startswith('__'))
        self.log.info("Topics (%d): %s", len(user_topics), user_topics)

        missing = [t for t in self.topics if t not in metadata.topics]
        if missing:
            raise AirflowException(f"Topic không tồn tại: {missing}")

        return brokers, {t: metadata.topics[t] for t in self.topics}

    # 3. Partition offset (watermark) -----------------------------------------
    def _get_watermarks(self, admin, topics, issues):
        watermarks = {}
        for topic, topic_md in topics.items():
            for p in topic_md.partitions.values():
                if p.leader == -1:
                    issues.append(f"Topic {topic} partition {p.id} không có leader")

            tps = [TopicPartition(topic, p) for p in sorted(topic_md.partitions)]
            low = admin.list_offsets({tp: OffsetSpec.earliest() for tp in tps})
            high = admin.list_offsets({tp: OffsetSpec.latest() for tp in tps})

            watermarks[topic] = {}
            for tp in tps:
                lo = low[tp].result(timeout=self.timeout).offset
                hi = high[tp].result(timeout=self.timeout).offset
                # key là str vì XCom serialize sang JSON
                watermarks[topic][str(tp.partition)] = {'low': lo, 'high': hi}
                self.log.info("%s[%d] low=%d high=%d messages=%d", topic, tp.partition, lo, hi, hi - lo)
        return watermarks

    # 4. Consumer group -------------------------------------------------------
    def _check_consumer_groups(self, admin, watermarks, issues):
        listed = admin.list_consumer_groups().result(timeout=self.timeout)
        group_ids = sorted({g.group_id for g in listed.valid} | set(self.consumer_groups))
        self.log.info("Consumer groups (%d): %s", len(group_ids), group_ids)

        groups = {}
        if not group_ids:
            return groups

        descriptions = admin.describe_consumer_groups(group_ids)
        for group_id in group_ids:
            try:
                desc = descriptions[group_id].result(timeout=self.timeout)
                state, members = desc.state.name, len(desc.members)
            except KafkaException as e:
                state, members = f'UNKNOWN ({e})', 0

            offsets = admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(group_id)])[group_id].result(timeout=self.timeout)

            # Consumer đã đọc đến đâu + lag = high watermark - committed offset
            committed = {}
            for tp in offsets.topic_partitions:
                if tp.topic not in watermarks or tp.offset < 0:
                    continue
                high = watermarks[tp.topic][str(tp.partition)]['high']
                committed.setdefault(tp.topic, {})[str(tp.partition)] = {
                    'committed': tp.offset,
                    'lag': high - tp.offset,
                }

            groups[group_id] = {'state': state, 'members': members, 'offsets': committed}
            self.log.info("Group %s state=%s members=%d offsets=%s", group_id, state, members, committed)

            if group_id in self.consumer_groups and state != 'STABLE':
                issues.append(f"Consumer group {group_id} không ở trạng thái STABLE (state={state})")

        missing = set(self.consumer_groups) - set(g.group_id for g in listed.valid)
        for group_id in sorted(missing):
            issues.append(f"Consumer group {group_id} không tồn tại / không còn active")

        return groups

    # 5. Spark streaming consumer (offset trong checkpoint) ---------------------
    def _check_spark_checkpoints(self, watermarks, issues):
        if not self.spark_checkpoints:
            return {}

        conn = BaseHook.get_connection(self.webhdfs_conn_id)
        webhdfs = f"{conn.host.rstrip('/')}:{conn.port}/webhdfs/v1"

        consumers = {}
        for name, path in self.spark_checkpoints.items():
            try:
                # commits/<batch_id> = batch đã xử lý xong; offsets/<batch_id> = offset cuối của batch đó
                statuses = requests.get(f"{webhdfs}{path}/commits?op=LISTSTATUS",
                                        timeout=self.timeout).json()['FileStatuses']['FileStatus']
                last_batch = max(int(s['pathSuffix']) for s in statuses if s['pathSuffix'].isdigit())
                content = requests.get(f"{webhdfs}{path}/offsets/{last_batch}?op=OPEN",
                                       timeout=self.timeout).text
            except (requests.RequestException, KeyError, ValueError) as e:
                issues.append(f"Không đọc được checkpoint của Spark consumer {name} ({path}): {e}")
                continue

            # File gồm 3 dòng: "v1", metadata, {"<topic>": {"<partition>": <offset>}}
            topic_offsets = json.loads(content.strip().splitlines()[-1])
            offsets = {}
            for topic, partitions in topic_offsets.items():
                if topic not in watermarks:
                    continue
                offsets[topic] = {
                    p: {'committed': offset, 'lag': watermarks[topic][p]['high'] - offset}
                    for p, offset in partitions.items()
                }

            consumers[name] = {'state': 'SPARK_CHECKPOINT', 'members': 1, 'last_batch': last_batch,
                               'offsets': offsets}
            self.log.info("Spark consumer %s batch=%d offsets=%s", name, last_batch, offsets)
        return consumers
