"""
日志生成器 — 程序入口

用法:
    # 输出到 Kafka（每分钟 1 万条，持续 1 小时）
    python main.py --rate 10000 --duration 3600 --output kafka

    # 输出到本地 JSON 文件（每分钟 1000 条，持续 60 秒，方便调试）
    python main.py --rate 1000 --duration 60 --output file

    # 生成 10 条直接打印到屏幕上（调试用）
    python main.py --count 10 --output stdout
"""

import sys
import time
import signal
from datetime import datetime
from typing import Optional

import click
from tqdm import tqdm

from config import OUTPUT_FILE_PATH
from event_factory import EventFactory
from models import UserBehaviorEvent


# ============================================================
# 全局开关：用于优雅退出
# ============================================================
running = True


def signal_handler(sig, frame):
    """捕获 Ctrl+C 信号，设置退出标志"""
    global running
    running = False
    print("\n⏹  收到停止信号，正在退出...")


signal.signal(signal.SIGINT, signal_handler)


# ============================================================
# 生成逻辑
# ============================================================

def run_generator(
    factory: EventFactory,
    rate_per_minute: int,
    duration_seconds: int,
    output: str,
    batch_size: int = 1000,
):
    """
    运行生成器的主循环。

    参数:
        factory:          事件工厂
        rate_per_minute:  每分钟生成多少条
        duration_seconds: 持续多长时间(秒)
        output:           输出目标: kafka / file / stdout
        batch_size:       每批生成多少条后输出一次
    """
    global running

    # ── 计算总量 ──
    total_events = int(rate_per_minute * duration_seconds / 60)
    print(f"\n{'='*50}")
    print(f"  开始生成日志")
    print(f"  速率: {rate_per_minute:,} 条/分钟")
    print(f"  时长: {duration_seconds} 秒")
    print(f"  预计总量: {total_events:,} 条")
    print(f"  输出目标: {output}")
    print(f"{'='*50}\n")

    # ── 初始化输出通道 ──
    if output == "kafka":
        from kafka_producer import EventKafkaProducer
        producer = EventKafkaProducer()
        producer.start()

        def write_batch(events):
            return producer.send_batch(events)

    elif output == "file":
        from kafka_producer import FileWriter
        producer = FileWriter(OUTPUT_FILE_PATH)
        producer.open()

        def write_batch(events):
            return producer.write_batch(events)

    else:  # stdout
        producer = None

        def write_batch(events):
            for e in events:
                print(e.to_json())
            return len(events)

    # ── 计算每次循环的时间间隔 ──
    events_per_second = rate_per_minute / 60.0
    events_per_batch = int(events_per_second * 0.5)  # 每 0.5 秒生成一批
    if events_per_batch < 1:
        events_per_batch = 1
    sleep_interval = 0.5

    # ── 进度条 ──
    pbar = tqdm(total=total_events, unit="条", desc="生成进度")

    # ── 主循环 ──
    generated = 0
    start_time = time.time()

    try:
        while running and generated < total_events:
            # 检查是否超时
            elapsed = time.time() - start_time
            if elapsed >= duration_seconds:
                break

            # 生成一批
            this_batch = min(events_per_batch, total_events - generated)
            base_time = datetime.now()
            events = factory.generate_batch(this_batch, base_time=base_time)

            # 输出
            written = write_batch(events)
            generated += written
            pbar.update(written)

            # 休眠控制速率
            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        print("\n⏹  收到中断信号")
    finally:
        pbar.close()

        # 清理
        if output == "kafka":
            producer.stop()
        elif output == "file":
            producer.close()

        # 统计
        actual_elapsed = time.time() - start_time
        actual_rate = generated / actual_elapsed * 60 if actual_elapsed > 0 else 0
        print(f"\n📊 生成统计:")
        print(f"   总计: {generated:,} 条")
        print(f"   耗时: {actual_elapsed:.1f} 秒")
        print(f"   实际速率: {actual_rate:,.0f} 条/分钟")


# ============================================================
# CLI 入口（使用 Click 库）
# ============================================================

@click.command()
@click.option(
    "--rate", "-r",
    default=10000,
    help="生成速率（条/分钟），默认 10000",
)
@click.option(
    "--duration", "-d",
    default=60,
    help="运行时长（秒），默认 60",
)
@click.option(
    "--count", "-c",
    default=None,
    type=int,
    help="生成固定条数后退出（覆盖 --rate 和 --duration）",
)
@click.option(
    "--output", "-o",
    default="stdout",
    type=click.Choice(["kafka", "file", "stdout"]),
    help="输出目标: kafka / file / stdout（默认 stdout）",
)
@click.option(
    "--batch-size", "-b",
    default=1000,
    help="批量发送大小（默认 1000）",
)
def main(rate, duration, count, output, batch_size):
    """
    用户行为埋点日志生成器

    模拟移动应用商店（OPPO/vivo）的百万级用户行为数据。
    输出到 Kafka、本地文件，或直接打印到终端。
    """

    # ── 创建事件工厂 ──
    factory = EventFactory()

    # ── 如果是固定条数模式 ──
    if count is not None:
        print(f"🎯 生成 {count} 条事件...\n")

        if output == "kafka":
            from kafka_producer import EventKafkaProducer
            producer = EventKafkaProducer()
            producer.start()
            for i in tqdm(range(0, count, batch_size), desc="生成"[:]):
                batch = factory.generate_batch(min(batch_size, count - i))
                producer.send_batch(batch)
            producer.stop()

        elif output == "file":
            from kafka_producer import FileWriter
            writer = FileWriter(OUTPUT_FILE_PATH)
            writer.open()
            for i in tqdm(range(0, count, batch_size), desc="生成"):
                batch = factory.generate_batch(min(batch_size, count - i))
                writer.write_batch(batch)
            writer.close()

        else:
            for i in tqdm(range(0, count, batch_size), desc="生成"):
                batch = factory.generate_batch(min(batch_size, count - i))
                for e in batch:
                    print(e.to_json())

        print(f"\n✅ 完成！共生成 {count} 条事件")
        return

    # ── 持续生成模式 ──
    run_generator(
        factory=factory,
        rate_per_minute=rate,
        duration_seconds=duration,
        output=output,
        batch_size=batch_size,
    )


if __name__ == "__main__":
    main()
