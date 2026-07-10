"""
Kafka 生产者 — 把生成的事件发送到 Kafka Topic

发送模式：
- 同步模式（sync=True）：发一条，等确认，再发下一条
  → 可靠但慢，用于调试
- 异步模式（sync=False，默认）：攒够一批再发
  → 吞吐量高，用于生产
"""

import json
import time
from typing import List

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import KAFKA_CONFIG
from models import UserBehaviorEvent


class EventKafkaProducer:
    """
    事件 Kafka 生产者

    用法:
        producer = EventKafkaProducer()
        producer.start()  # 建立连接

        # 发送单条
        producer.send_event(event)

        # 批量发送
        producer.send_batch(events)

        producer.stop()  # 关闭连接
    """

    def __init__(self):
        self.producer: KafkaProducer = None
        self.topic = KAFKA_CONFIG["topic"]
        self.bootstrap_servers = KAFKA_CONFIG["bootstrap_servers"]

    def start(self):
        """
        建立 Kafka 连接。

        这一步会真正去连 localhost:9092。
        如果 Kafka 没启动，这里会报错。
        """
        print(f"📡 连接 Kafka: {self.bootstrap_servers}")
        print(f"📨 Topic: {self.topic}")

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            # 数据序列化：Python dict → JSON 字符串 → bytes（Kafka 只认 bytes）
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            # 键序列化：event_type 做 key，方便 Flink 下游做分区
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            # ===== 性能配置 =====
            # 攒够 batch_size 条才发（配合下面的 linger_ms）
            batch_size=KAFKA_CONFIG.get("batch_size", 16384),
            # 最多等 linger_ms 毫秒，超时即使不够 batch_size 也发
            linger_ms=KAFKA_CONFIG.get("linger_ms", 100),
            # 压缩方式（减少网络传输量）
            compression_type="gzip",
            # 发送失败重试次数
            retries=3,
            # 确认机制：all = 所有副本都确认才认为成功
            acks="all",
        )
        print("✅ Kafka 连接成功")

    def send_event(self, event: UserBehaviorEvent) -> bool:
        """
        发送单条事件到 Kafka。

        返回: True=成功, False=失败
        """
        data = event.to_dict()
        key = event.event_type  # 按事件类型分区

        try:
            # 异步发送（默认），返回 Future 对象
            future = self.producer.send(
                topic=self.topic,
                key=key,
                value=data,
            )
            # 等待确认（如果需要同步的话加 .get()）
            # future.get(timeout=10)  # 阻塞等待，超时抛异常
            return True
        except KafkaError as e:
            print(f"❌ Kafka 发送失败: {e}")
            return False

    def send_batch(self, events: List[UserBehaviorEvent]) -> int:
        """
        批量发送。

        返回: 成功发送的条数
        """
        success_count = 0
        for event in events:
            if self.send_event(event):
                success_count += 1

        # flush: 强制把缓冲区里攒着的全部发出去
        self.producer.flush()
        return success_count

    def stop(self):
        """关闭 Kafka 连接"""
        if self.producer:
            print("🔌 关闭 Kafka 连接...")
            self.producer.flush()   # 先把积压的发完
            self.producer.close()   # 再断开连接
            print("✅ Kafka 连接已关闭")


class FileWriter:
    """
    本地文件写入器 — 作为 Kafka 的替代方案

    当你不方便启动 Kafka 时（比如在火车上写代码），
    用这个把日志写到本地 JSONL 文件。

    JSONL = JSON Lines，每行一条 JSON，方便逐行读取。

    用法:
        writer = FileWriter("../../data/sample_events.jsonl")
        writer.open()
        writer.write_batch(events)
        writer.close()
    """

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file_handler = None

    def open(self):
        self.file_handler = open(self.file_path, "w", encoding="utf-8")
        print(f"📄 输出到文件: {self.file_path}")

    def write_batch(self, events: List[UserBehaviorEvent]) -> int:
        """写入一批事件，返回写入条数"""
        count = 0
        for event in events:
            self.file_handler.write(event.to_json() + "\n")
            count += 1
        self.file_handler.flush()  # 立即刷盘，防止意外中断丢数据
        return count

    def close(self):
        if self.file_handler:
            self.file_handler.close()
            print(f"📄 文件已保存: {self.file_path}")
