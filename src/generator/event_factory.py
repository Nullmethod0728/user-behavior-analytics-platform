"""
事件工厂 — 生成器的核心

负责把随机数据组合成一条条看起来真实的行为日志。

设计思路：
1. 维护一个用户池（5000人），模拟用户在一天中反复活跃
2. 每个用户有独立的 session（会话），session 内事件时间递增
3. 不同事件类型携带不同的 properties（下载事件有app信息，搜索事件有关键词）
"""

import random
import uuid
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

from faker import Faker

from config import (
    EVENT_TYPES,
    DEVICE_MODELS,
    OS_VERSIONS,
    APP_VERSIONS,
    PAGES,
    CITIES,
    APP_CATALOG,
    SEARCH_KEYWORDS,
    USER_POOL_SIZE,
    DEVICE_POOL_SIZE,
    EXPERIMENTS,
    AB_EXPERIMENT_ENABLED,
)
from models import UserBehaviorEvent

# 初始化 Faker（中文模式，生成中文城市名等）
fake = Faker("zh_CN")


# ============================================================
# 辅助函数：加权随机选择
# ============================================================
# Python 的 random.choices 支持 weights 参数
# 比如 [("A", 60), ("B", 40)] → A 有 60% 概率被选中
# 把 config.py 中的 dict 转成 (key, weight) 的列表传给 random.choices

def _weighted_choice(weighted_dict: Dict[str, float]) -> str:
    """从带权重的字典中随机选一个key"""
    items = list(weighted_dict.keys())
    weights = list(weighted_dict.values())
    return random.choices(items, weights=weights, k=1)[0]


def _weighted_choice_from_tuple(weighted_list: list, value_index=None, weight_index: int = -1) -> any:
    """
    从带权重的 tuple 列表中随机选一个值。

    比如 DEVICE_MODELS 里每个元素是 (品牌, 型号, 权重)
    - value_index=1 → 只选型号（返回单个字符串）
    - value_index=(0, 1) → 同时选品牌和型号（返回 tuple）
    - value_index=None → 默认返回整行（去掉权重列）

    weight_index=-1 用最后一个元素做权重
    """
    weights = [item[weight_index] for item in weighted_list]
    # 随机选一行
    picked = random.choices(weighted_list, weights=weights, k=1)[0]

    if value_index is None:
        # 返回整行去掉权重
        return picked[:weight_index] if weight_index == -1 else picked[:weight_index] + picked[weight_index+1:]

    if isinstance(value_index, tuple):
        # 同时取多个字段，返回 tuple
        return tuple(picked[i] for i in value_index)

    # 取单个字段
    return picked[value_index]


# ============================================================
# 主类：EventFactory
# ============================================================

class EventFactory:
    """
    事件工厂

    用法:
        factory = EventFactory()

        # 生成一条事件
        event = factory.generate_event()

        # 生成 100 条
        events = factory.generate_batch(100)
    """

    def __init__(self):
        # ── 预生成用户池和设别池 ──
        # 不在每次生成事件时才随机，而是提前建好池子
        # 这样同一个 user_id 会在不同时间反复出现 → 模拟留存
        print(f"🔧 初始化事件工厂...")
        print(f"   用户池: {USER_POOL_SIZE} 人")
        print(f"   设备池: {DEVICE_POOL_SIZE} 台")

        self.users = [f"user_{i:06d}" for i in range(1, USER_POOL_SIZE + 1)]
        self.devices = [f"device_{i:06d}" for i in range(1, DEVICE_POOL_SIZE + 1)]

        # ── AB 实验：预计算每个用户的分组 ──
        # 关键设计：用 hash(user_id) 做确定性分桶
        #   同一个 user_id 永远落在同一分组 → 用户体验一致
        #   不同实验之间用户分组独立（用 experiment_id 参与 hash）
        self.user_variants: dict = {}  # {(user_id, experiment_id): variant}
        if AB_EXPERIMENT_ENABLED:
            for exp_id, exp_config in EXPERIMENTS.items():
                a_pct = exp_config["traffic_split"]["A"]
                for user_id in self.users:
                    # hashlib.md5(user_id + experiment_id) → 每个实验独立分桶
                    # 用 md5 替代 hash() 的原因是：Python 的 hash() 每次启动
                    # 解释器都会变化（PYTHONHASHSEED），导致同一用户在不同
                    # 运行时被分到不同组——这在 AB 实验中是致命的
                    hash_bytes = hashlib.md5(
                        f"{user_id}_{exp_id}".encode("utf-8")
                    ).digest()
                    bucket = int.from_bytes(hash_bytes[:4], "big") % 100
                    variant = "A" if bucket < a_pct else "B"
                    self.user_variants[(user_id, exp_id)] = variant
            print(f"   AB 实验: {len(EXPERIMENTS)} 个实验已启用")
            for exp_id, exp_config in EXPERIMENTS.items():
                labels = exp_config["variant_labels"]
                print(f"     {exp_id}: {labels['A']} vs {labels['B']}")

        # ── 给每台设备预设属性 ──
        # 一台设备的型号、系统版本是固定的（不会今天OPPO明天vivo）
        self.device_info: Dict[str, dict] = {}
        for device_id in self.devices:
            brand, model = _weighted_choice_from_tuple(DEVICE_MODELS, value_index=(0, 1), weight_index=2)
            # ↑ 我们想要一次同时拿到 (品牌, 型号) 两个字段，所以用 value_index 来指定合并返回
            # 重构：分开处理
            self.device_info[device_id] = {
                "brand": brand,
                "model": model,
                "os_version": _weighted_choice(OS_VERSIONS),
            }

        # ── 用户会话状态 ──
        # 每个活跃用户当前属于哪个 session
        self.user_sessions: Dict[str, str] = {}

        # 每个 session 的最后一条事件时间（用于时间递增）
        self.session_last_time: Dict[str, datetime] = {}

        # session 过期时间（超过这个时间没活动就开新 session）
        self.session_timeout = timedelta(minutes=30)

        print(f"✅ 事件工厂初始化完成")

    # ── 设备属性获取 ──

    def _get_device_info(self) -> Tuple[str, str, str]:
        """
        随机分配一台设备并返回 (设备ID, 设备型号, 系统版本)

        真实场景规律：高端机型用户更活跃 → 让高权重的设备更多被分到用户
        这里不做复杂建模，均匀随机分配。
        """
        # 多台设备共享一个设备信息的概率（现实中一台设备被多人使用）
        # 为了简单，大多数用户有自己独立的设备
        if random.random() < 0.85:  # 85% 概率：随机一个独立设备
            device_id = random.choice(self.devices)
        else:  # 15% 概率：随机一个用户，然后用该用户的设备（模拟多人共用）
            # 实际上 device 是按设备属性绑定的，这里简化处理
            device_id = random.choice(self.devices)

        info = self.device_info[device_id]
        return device_id, info["model"], info["os_version"]

    # ── 城市 + IP 生成 ──

    def _get_location(self) -> Tuple[str, str]:
        """随机生成 (城市, IP)"""
        city = _weighted_choice(CITIES)
        ip = fake.ipv4()
        return city, ip

    # ── Session 管理 ──

    def _get_session(self, user_id: str, event_time: datetime) -> str:
        """
        管理用户的 session。

        规则：
        - 如果用户当前没有活跃 session → 新建一个
        - 如果用户距离上次活动超过 30 分钟 → 新建一个（旧 session 过期）
        - 否则 → 继续使用当前 session
        """
        if user_id in self.user_sessions:
            last_time = self.session_last_time.get(self.user_sessions[user_id])
            if last_time and (event_time - last_time) < self.session_timeout:
                # session 仍然活跃
                self.session_last_time[self.user_sessions[user_id]] = event_time
                return self.user_sessions[user_id]

        # 需要新 session
        new_session = f"sess_{uuid.uuid4().hex[:12]}"
        self.user_sessions[user_id] = new_session
        self.session_last_time[new_session] = event_time
        return new_session

    # ── Properties 生成（按事件类型） ──

    def _generate_properties(self, event_type: str) -> dict:
        """
        根据事件类型生成对应的扩展属性。

        不同的事件类型携带不同的业务信息：
        - 下载/安装: 带 app_id、app_name、下载来源
        - 搜索: 带搜索关键词
        - 浏览/点击: 可能带被浏览的 app 信息
        """
        props = {}

        if event_type == "app_download":
            app_tuple = random.choices(
                APP_CATALOG,
                weights=[a[3] for a in APP_CATALOG],
                k=1
            )[0]
            props = {
                "app_id": app_tuple[0],
                "app_name": app_tuple[1],
                "app_category": app_tuple[2],
                "download_source": random.choice(["search", "recommend", "ranking", "banner"]),
            }

        elif event_type == "app_install":
            app_tuple = random.choices(
                APP_CATALOG,
                weights=[a[3] for a in APP_CATALOG],
                k=1
            )[0]
            props = {
                "app_id": app_tuple[0],
                "app_name": app_tuple[1],
                "app_category": app_tuple[2],
                "install_result": random.choice(["success", "success", "success", "fail"]),
                # ↑ 大部分安装成功，偶尔失败
            }

        elif event_type == "search_query":
            props = {
                "keyword": random.choice(SEARCH_KEYWORDS),
                "search_type": random.choice(["text", "voice", "image"]),
            }

        elif event_type == "app_click":
            app_tuple = random.choices(
                APP_CATALOG,
                weights=[a[3] for a in APP_CATALOG],
                k=1
            )[0]
            props = {
                "app_id": app_tuple[0],
                "app_name": app_tuple[1],
                "position": random.randint(1, 20),  # 在列表中的位置
            }

        elif event_type == "ad_impression":
            props = {
                "ad_id": f"ad_{random.randint(1000, 9999)}",
                "ad_type": random.choice(["banner", "interstitial", "native", "video"]),
                "ad_position": random.choice(["home_top", "detail_bottom", "search_result"]),
            }

        elif event_type == "ad_click":
            props = {
                "ad_id": f"ad_{random.randint(1000, 9999)}",
                "ad_type": random.choice(["banner", "interstitial", "native", "video"]),
            }

        elif event_type == "in_app_purchase":
            props = {
                "order_id": f"order_{uuid.uuid4().hex[:16]}",
                "amount": round(random.uniform(1, 648), 2),  # 1元到648元（参考手游充值档位）
                "currency": "CNY",
            }

        return props

    # ── AB 实验分桶 ──

    def _get_experiment_assignment(self, user_id: str) -> Tuple[str, str]:
        """
        获取用户在实验中的分组。

        返回: (experiment_id, variant)
          - 如果用户参与多个实验，随机选一个正在进行的实验
          - 如果 AB 实验关闭，返回空字符串

        核心机制：hash(user_id + experiment_id) % 100 确定性分桶
          同一个用户永远落同一组 → 用户体验一致
          不同实验独立 hash → 实验之间互不干扰
        """
        if not AB_EXPERIMENT_ENABLED or not EXPERIMENTS:
            return "", ""

        # 简单策略：随机选一个实验，实际生产环境中
        # 一个用户会同时参与多个实验（正交分流）
        exp_id = random.choice(list(EXPERIMENTS.keys()))
        variant = self.user_variants.get((user_id, exp_id), "")
        return exp_id, variant

    # ── 核心方法：生成一条事件 ──

    def generate_event(self, base_time: Optional[datetime] = None) -> UserBehaviorEvent:
        """
        生成一条完整的用户行为事件。

        参数:
            base_time: 基准时间。如果不传就用当前时间。
                       批量生成时传入，让所有事件时间都在 base_time 附近。

        返回:
            UserBehaviorEvent 对象
        """
        # ── 1. 选事件类型 ──
        event_type = _weighted_choice(EVENT_TYPES)

        # ── 2. 选用户 ──
        user_id = random.choice(self.users)

        # ── 3. 选设备 ──
        device_id, device_model, os_version = self._get_device_info()

        # ── 4. 时间 ──
        if base_time is None:
            base_time = datetime.now()
        # 在 base_time 的前后 5 秒内随机偏移（模拟真实时间分布）
        offset = timedelta(seconds=random.randint(-5, 5))
        event_time = base_time + offset
        server_time = event_time + timedelta(milliseconds=random.randint(50, 500))
        # ↑ server_time 比 event_time 晚一点点（网络传输延迟）

        # ── 5. Session ──
        session_id = self._get_session(user_id, event_time)

        # ── 6. 页面 ──
        page = random.choice(PAGES)

        # ── 7. 位置 ──
        city, ip = self._get_location()

        # ── 8. App 版本 ──
        app_version = random.choice(APP_VERSIONS)

        # ── 9. 停留时长 ──
        if event_type == "page_view":
            duration = random.randint(500, 120000)  # 500ms ~ 2分钟
        else:
            duration = random.randint(0, 5000)

        # ── 10. Properties ──
        properties = self._generate_properties(event_type)

        # ── 11. AB 实验分组 ──
        experiment_id, variant = self._get_experiment_assignment(user_id)

        # ── 组装 ──
        event = UserBehaviorEvent(
            event_type=event_type,
            user_id=user_id,
            device_id=device_id,
            event_time=event_time,
            server_time=server_time,
            device_model=device_model,
            os_version=os_version,
            app_version=app_version,
            session_id=session_id,
            page=page,
            ip=ip,
            city=city,
            duration=duration,
            properties=properties,
            source="simulator",
            experiment_id=experiment_id,
            variant=variant,
        )

        return event

    def generate_batch(self, count: int, base_time: Optional[datetime] = None) -> list:
        """
        批量生成事件。

        参数:
            count: 生成数量
            base_time: 基准时间

        返回:
            UserBehaviorEvent 列表
        """
        if base_time is None:
            base_time = datetime.now()

        events = []
        for i in range(count):
            # base_time 随着生成进度缓慢推进（模拟时间的流逝）
            # 如果 rate=100000/min，每毫秒约 1.67 条
            # 这里简化：每条事件时间约等于 base_time（偏差5秒内）
            event = self.generate_event(base_time=base_time)
            events.append(event)

            # 每生成若干条，时间推进 1 秒（模拟密集事件流）
            if i % 100 == 0:
                base_time += timedelta(milliseconds=random.randint(500, 1500))

        return events
