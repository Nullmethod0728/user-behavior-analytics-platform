"""
AB 实验显著性检验脚本

从 ClickHouse 读取 AB 实验各组指标，计算 P 值，
判断实验效果是否统计显著，结果写回 ClickHouse。

调用方式：
  python significance_test.py --date 2026-07-10

技术要点（面试必问）：
  1. 零假设 H₀: A组和B组的指标没有差异
  2. P 值: 在 H₀ 成立的前提下，观察到当前数据（或更极端）的概率
  3. P < 0.05 → 拒绝 H₀ → 差异统计显著 → 实验组确实有效
  4. P ≥ 0.05 → 不能拒绝 H₀ → 观察到的差异可能是随机波动
  5. 第一类错误（α）: H₀ 为真但被错误拒绝 → P<0.05 时的误判风险 5%
  6. 第二类错误（β）: H₀ 为假但未拒绝 → 样本量不足时的漏判风险
"""

import argparse
import json
import sys
from datetime import date, timedelta
from typing import Dict, List, Tuple

from clickhouse_driver import Client

# ── 尝试导入 scipy，如果没有安装就用简化的近似方法 ──
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    print("⚠️ scipy 未安装，将使用简化 Z 检验。安装: pip install scipy")


# ============================================================
# ClickHouse 连接
# ============================================================
CK_CLIENT = Client(
    host="localhost",
    port=9000,
    database="analytics",
    user="admin",
    password="admin123",
)


def get_experiment_sample(experiment_id: str, variant: str, stat_date: str,
                           metric_name: str) -> List[float]:
    """
    获取某实验某分组某指标的原始样本数据。

    真实 AB 实验中，样本数据通常从明细表抽取。
    这里从 DWD 表按用户粒度聚合，每个用户一行作为独立样本。

    采样策略：随机抽取最多 500 个样本（足够计算 P 值）
    """
    if metric_name in ("pv", "download_count", "search_count"):
        # 计数类指标：每个用户的事件数
        query = """
            SELECT user_id, count() AS value
            FROM analytics.dwd_user_behavior
            WHERE experiment_id = %(exp_id)s
              AND variant = %(variant)s
              AND toDate(event_time) = %(date)s
            GROUP BY user_id
            ORDER BY rand()
            LIMIT 500
        """
    elif metric_name == "uv":
        # UV 只有一个值，不需要抽样
        return []
    elif metric_name == "download_conversion_rate":
        # 转化率：每个用户是否有下载行为 (0/1)
        query = """
            SELECT
                user_id,
                max(CASE WHEN event_type = 'app_download' THEN 1 ELSE 0 END) AS value
            FROM analytics.dwd_user_behavior
            WHERE experiment_id = %(exp_id)s
              AND variant = %(variant)s
              AND toDate(event_time) = %(date)s
            GROUP BY user_id
            LIMIT 500
        """
    elif metric_name == "avg_duration":
        # 连续指标：每个用户的平均停留时长
        query = """
            SELECT user_id, avg(duration) AS value
            FROM analytics.dwd_user_behavior
            WHERE experiment_id = %(exp_id)s
              AND variant = %(variant)s
              AND toDate(event_time) = %(date)s
              AND event_type = 'page_view'
            GROUP BY user_id
            LIMIT 500
        """
    else:
        return []

    try:
        rows = CK_CLIENT.execute(query, {
            "exp_id": experiment_id,
            "variant": variant,
            "date": stat_date,
        })
        return [row[1] for row in rows]
    except Exception as e:
        print(f"  ⚠️ 获取样本数据失败 ({experiment_id}/{variant}/{metric_name}): {e}")
        return []


def calc_p_value(sample_a: List[float], sample_b: List[float],
                 metric_name: str) -> float:
    """
    计算 P 值。

    检验方法选择：
    - 转化率等 0/1 二值指标 → Fisher's Exact / Chi-Square
    - 连续指标（时长等）→ Welch's t-test（不假设方差相等）
    - 计数指标 → Mann-Whitney U test（不假设正态分布）

    返回: P 值，-1 表示无法计算
    """
    if len(sample_a) < 5 or len(sample_b) < 5:
        return -1  # 样本太小，无法计算

    # 检测是否为二值指标（0/1）
    is_binary = all(v in (0, 1) for v in sample_a + sample_b)

    if SCIPY_AVAILABLE:
        try:
            if is_binary:
                # Fisher 精确检验 → 对二值小样本更精确
                table = [
                    [sum(sample_a), len(sample_a) - sum(sample_a)],
                    [sum(sample_b), len(sample_b) - sum(sample_b)],
                ]
                _, p = stats.fisher_exact(table)
            elif metric_name == "avg_duration":
                # Welch's t-test → 不假设方差相等
                _, p = stats.ttest_ind(sample_a, sample_b, equal_var=False)
            else:
                # Mann-Whitney U test → 非参数检验，不假设正态分布
                _, p = stats.mannwhitneyu(sample_a, sample_b, alternative="two-sided")
            return round(p, 6)
        except Exception as e:
            print(f"  ⚠️ 显著性检验失败: {e}")
            return -1
    else:
        # ── 简化版 Z 检验（无 scipy 时的备选方案）──
        return _simplified_z_test(sample_a, sample_b)


def _simplified_z_test(sample_a: List[float], sample_b: List[float]) -> float:
    """
    简化版两样本 Z 检验。

    原理：Z = (mean_a - mean_b) / sqrt(var_a/n_a + var_b/n_b)
    P = 2 * (1 - Φ(|Z|))  即标准正态分布的双尾概率

    面试要点：
      - Z 检验要求样本量较大（n>30），且总体方差已知或可估计
      - 如果样本量小或数据偏态，应该用 t 检验或非参数检验
      - 实际项目中一定用 scipy.stats，不要手写统计检验
    """
    import math

    n_a, n_b = len(sample_a), len(sample_b)
    mean_a = sum(sample_a) / n_a
    mean_b = sum(sample_b) / n_b
    var_a = sum((x - mean_a) ** 2 for x in sample_a) / (n_a - 1)
    var_b = sum((x - mean_b) ** 2 for x in sample_b) / (n_b - 1)

    se = math.sqrt(var_a / n_a + var_b / n_b)  # 标准误 (Standard Error)
    if se == 0:
        return 1.0  # 两组完全一样

    z = abs(mean_a - mean_b) / se

    # 标准正态分布的近似 P 值（用多项式逼近）
    # 精确计算需要 scipy.stats.norm.sf()
    t = 1.0 / (1.0 + 0.2316419 * z)
    poly = t * (0.319381530 + t * (-0.356563782 + t * (
        1.781477937 + t * (-1.821255978 + t * 1.330274429))))
    p = round(2 * poly * math.exp(-z * z / 2), 6)

    return p


def significance_test_runner(stat_date: str):
    """
    主流程：读取 AB 指标 → 拉样本 → 算 P 值 → 写回 ClickHouse
    """

    print("=" * 60)
    print(f"  AB 实验显著性检验")
    print(f"  统计日期: {stat_date}")
    print(f"  scipy: {'✅ 已安装' if SCIPY_AVAILABLE else '⚠️ 使用简化Z检验'}")
    print("=" * 60)

    # ── 1. 查询所有实验指标 ──
    query = """
        SELECT experiment_id, variant, metric_name, metric_value, user_count
        FROM analytics.ads_ab_experiment
        WHERE stat_date = %(date)s
        ORDER BY experiment_id, metric_name, variant
    """
    rows = CK_CLIENT.execute(query, {"date": stat_date})

    if not rows:
        print("⚠️ 当日没有 AB 实验数据，请先运行 Spark 作业")
        return

    # ── 2. 按 (experiment_id, metric_name) 成对处理 ──
    experiments: Dict[str, Dict[str, Dict[str, float]]] = {}
    for row in rows:
        exp_id, variant, metric_name, value, users = row
        experiments.setdefault(exp_id, {}).setdefault(metric_name, {})[variant] = {
            "value": value,
            "users": users,
        }

    results = []

    for exp_id, metrics in experiments.items():
        print(f"\n🧪 实验: {exp_id}")

        for metric_name, variants in metrics.items():
            if "A" not in variants or "B" not in variants:
                continue  # 缺少对照组或实验组，跳过

            a_val = variants["A"]["value"]
            b_val = variants["B"]["value"]

            # ── 3. 拉取样本数据，计算 P 值 ──
            print(f"  📊 {metric_name}: A={a_val:.2f} vs B={b_val:.2f}", end="")

            sample_a = get_experiment_sample(exp_id, "A", stat_date, metric_name)
            sample_b = get_experiment_sample(exp_id, "B", stat_date, metric_name)

            p_value = calc_p_value(sample_a, sample_b, metric_name)
            is_significant = 1 if 0 < p_value < 0.05 else 0

            uplift = ((b_val - a_val) / a_val * 100) if a_val > 0 else 0

            sign = "✅" if is_significant else "❌"
            print(f" | P={p_value} {sign} uplift={uplift:+.2f}%")

            # ── 4. 写回 ClickHouse ──
            for variant in ("A", "B"):
                update_query = """
                    ALTER TABLE analytics.ads_ab_experiment
                    UPDATE p_value = %(p_value)s,
                           is_significant = %(is_significant)s,
                           uplift = %(uplift)s
                    WHERE experiment_id = %(exp_id)s
                      AND stat_date = %(date)s
                      AND variant = %(variant)s
                      AND metric_name = %(metric_name)s
                """
                try:
                    CK_CLIENT.execute(update_query, {
                        "p_value": p_value,
                        "is_significant": is_significant,
                        "uplift": uplift,
                        "exp_id": exp_id,
                        "date": stat_date,
                        "variant": variant,
                        "metric_name": metric_name,
                    })
                except Exception as e:
                    # ALTER TABLE UPDATE 在某些 ClickHouse 版本不支持
                    # 回退方案：用 INSERT ... SELECT 或后续处理
                    print(f"  ⚠️ 更新 ClickHouse 失败 ({variant}): {e}")

            results.append({
                "experiment_id": exp_id,
                "metric_name": metric_name,
                "a_value": a_val,
                "b_value": b_val,
                "uplift": uplift,
                "p_value": p_value,
                "is_significant": is_significant,
            })

    # ── 5. 生成报告 ──
    print("\n" + "=" * 60)
    print("  AB 实验分析报告")
    print("=" * 60)
    for r in results:
        sig = "✅ 显著" if r["is_significant"] else "❌ 不显著"
        print(f"\n  [{r['experiment_id']}] {r['metric_name']}")
        print(f"    A 组: {r['a_value']:.2f} | B 组: {r['b_value']:.2f}")
        print(f"    Uplift: {r['uplift']:+.2f}% | P-value: {r['p_value']} | {sig}")

    print("\n✅ 显著性检验全部完成")
    return results


def main():
    parser = argparse.ArgumentParser(description="AB 实验显著性检验")
    parser.add_argument("--date", type=str, default=None,
                        help="统计日期 (YYYY-MM-DD)，默认昨天")
    args = parser.parse_args()

    stat_date = args.date or (date.today() - timedelta(days=1)).isoformat()
    significance_test_runner(stat_date)


if __name__ == "__main__":
    main()
