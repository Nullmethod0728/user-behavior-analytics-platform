"""
数据模型定义

一个 UserBehaviorEvent 对象 = 一条埋点日志
所有字段与 ClickHouse ods_user_behavior_log 表严格对齐
"""

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional
from uuid import uuid4


@dataclass
class UserBehaviorEvent:
    """
    用户行为事件

    @dataclass 是 Python 3.7+ 的装饰器，自动帮你生成 __init__、__repr__ 等方法。
    你不需要手写 __init__(self, event_id, event_type, ...) 那一大坨。
    """

    # ── 事件标识 ──
    event_type: str                        # 事件类型: page_view / app_click / ...
    event_id: str = field(default_factory=lambda: str(uuid4()))
    # ↑ default_factory：如果不传 event_id，自动用 UUID 生成一个

    # ── 用户标识 ──
    user_id: str = ""
    device_id: str = ""

    # ── 时间 ──
    event_time: datetime = field(default_factory=datetime.now)
    server_time: datetime = field(default_factory=datetime.now)

    # ── 设备信息 ──
    device_model: str = ""                 # 如 "OPPO Find X6 Pro"
    os_version: str = ""                   # 如 "Android 14"
    app_version: str = ""                  # 如 "8.6.3"

    # ── 会话信息 ──
    session_id: str = ""

    # ── 页面信息 ──
    page: str = "home"

    # ── 位置信息 ──
    ip: str = ""
    city: str = ""

    # ── 行为数据 ──
    duration: int = 0                      # 页面停留时长(毫秒)

    # ── 扩展属性 ──
    properties: dict = field(default_factory=dict)

    # ── 数据来源标记 ──
    source: str = "simulator"

    # ============================================================
    # 序列化方法
    # ============================================================

    def to_dict(self) -> dict:
        """
        转为字典，用于 JSON 序列化

        示例输出:
        {
            "event_id": "evt_a1b2c3...",
            "event_type": "app_download",
            ...
        }
        """
        result = asdict(self)  # dataclass 自带方法，自动转 dict

        # datetime 对象需要转成字符串，JSON 不认识 datetime
        result["event_time"] = self.event_time.strftime("%Y-%m-%d %H:%M:%S")
        result["server_time"] = self.server_time.strftime("%Y-%m-%d %H:%M:%S")

        # properties 是 dict，转成 JSON 字符串存到 ClickHouse
        result["properties"] = json.dumps(self.properties, ensure_ascii=False)

        return result

    def to_json(self) -> str:
        """转为 JSON 字符串（发 Kafka 用这个格式）"""
        return json.dumps(self.to_dict(), ensure_ascii=False)
