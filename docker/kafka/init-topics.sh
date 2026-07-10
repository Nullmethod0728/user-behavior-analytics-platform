#!/bin/bash
# ============================================================
# Kafka Topic 初始化脚本
# 在 kafka-init 容器启动时自动执行
# ============================================================

echo "========================================="
echo "  等待 Kafka Broker 就绪..."
echo "========================================="

# 等待 Kafka 完全启动（最多等60秒）
# 我们不断尝试连接，直到成功或超时
RETRY_COUNT=0
MAX_RETRIES=30

while [[ $RETRY_COUNT -lt $MAX_RETRIES ]]; do
    # kafka-broker 状态检查：尝试获取集群ID
    if kafka-broker-api-versions --bootstrap-server kafka:9092 &>/dev/null; then
        echo "✅ Kafka Broker 就绪！"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo "⏳ 等待 Kafka 启动... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done

if [[ $RETRY_COUNT -eq $MAX_RETRIES ]]; then
    echo "❌ Kafka 启动超时，退出"
    exit 1
fi

echo ""
echo "========================================="
echo "  开始创建 Topic..."
echo "========================================="

# ==================== Topic 定义 ====================

# --- Topic 1: 原始埋点日志 ---
# 日志生成器把模拟的用户行为数据发送到这个Topic
#   partitions: 3  → 数据分散到3个分区，每个分区可独立消费
#   retention: 7天 → 7天后旧数据自动删除
kafka-topics --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic user-behavior-log \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000

echo "  ✅ [user-behavior-log]       原始埋点日志 (3分区 / 7天保留)"

# --- Topic 2: 清洗后的明细数据 ---
# Flink ETL作业处理后的干净数据写入这里
#   用途：供下游离线分析使用
kafka-topics --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic user-behavior-dwd \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000

echo "  ✅ [user-behavior-dwd]       清洗后明细数据 (3分区 / 7天保留)"

# --- Topic 3: 实时指标 ---
# Flink 计算出的实时指标（PV/UV/下载量等）写入这里
#   用途：Dashboard 消费此Topic做实时展示
kafka-topics --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic real-time-metrics \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=86400000

echo "  ✅ [real-time-metrics]       实时聚合指标 (1分区 / 1天保留)"

echo ""
echo "========================================="
echo "  Topic 创建完成！当前 Topic 列表："
echo "========================================="
kafka-topics --bootstrap-server kafka:9092 --list
