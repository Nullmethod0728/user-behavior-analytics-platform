/**
 * ECharts 图表渲染模块
 *
 * 5 个图表：
 *  1. KPI 翻牌器（纯 HTML，定时刷新）
 *  2. 24 小时趋势图（折线图）
 *  3. 漏斗图
 *  4. 留存率图（柱状图 + 折线）
 *  5. Top 下载量（横向柱状图）
 */

// ECharts 暗色主题通用配置
const DARK_THEME = {
    textStyle: { color: '#b0c4de' },
    grid: { top: 40, right: 30, bottom: 40, left: 60 },
};

// ============================================================
// 初始化所有图表实例
// ============================================================

const chartTrend = echarts.init(document.getElementById('chart-trend'));
const chartFunnel = echarts.init(document.getElementById('chart-funnel'));
const chartRetention = echarts.init(document.getElementById('chart-retention'));
const chartTopDownloads = echarts.init(document.getElementById('chart-top-downloads'));

// 窗口大小变化时自适应
window.addEventListener('resize', () => {
    chartTrend.resize();
    chartFunnel.resize();
    chartRetention.resize();
    chartAB.resize();
    chartTopDownloads.resize();
});

// ============================================================
// 1. KPI 翻牌器（定时更新）
// ============================================================

async function updateKPIs() {
    const metrics = await API.getRealtimeMetrics();
    if (!metrics) return;

    document.getElementById('kpi-pv').textContent = formatNumber(metrics.pv || 0);
    document.getElementById('kpi-uv').textContent = formatNumber(metrics.uv || 0);

    // 下载量和搜索量从日汇总取（Demo 模式随机生成）
    document.getElementById('kpi-download').textContent =
        formatNumber(Math.floor(Math.random() * 5000 + 3000));
    document.getElementById('kpi-search').textContent =
        formatNumber(Math.floor(Math.random() * 8000 + 5000));

    document.getElementById('last-update').textContent =
        new Date().toLocaleTimeString('zh-CN');
}

/** 数字格式化：128000 → "128,000" */
function formatNumber(n) {
    return n.toLocaleString('en-US');
}

// ============================================================
// 2. PV/UV 趋势图
// ============================================================

async function updateTrendChart() {
    const today = new Date();
    const from = new Date(today);
    from.setDate(from.getDate() - 7);
    const fromStr = from.toISOString().split('T')[0];
    const toStr = today.toISOString().split('T')[0];

    const data = await API.getDailyMetrics(fromStr, toStr);
    if (!data || data.length === 0) return;

    const dates = data.map(d => d.stat_date);
    const pvs = data.map(d => d.pv);
    const uvs = data.map(d => d.uv);

    chartTrend.setOption({
        tooltip: {
            trigger: 'axis',
            backgroundColor: 'rgba(10,14,39,0.9)',
            borderColor: '#4facfe',
            textStyle: { color: '#e0e6ed' }
        },
        legend: {
            data: ['PV', 'UV'],
            textStyle: { color: '#8899aa' },
            top: 0
        },
        grid: DARK_THEME.grid,
        xAxis: {
            type: 'category',
            data: dates,
            axisLabel: { color: '#8899aa', rotate: 30 },
            axisLine: { lineStyle: { color: '#2a3550' } }
        },
        yAxis: {
            type: 'value',
            axisLabel: { color: '#8899aa' },
            splitLine: { lineStyle: { color: '#1a2540' } }
        },
        series: [
            {
                name: 'PV',
                type: 'line',
                data: pvs,
                smooth: true,
                lineStyle: { color: '#4facfe', width: 2 },
                itemStyle: { color: '#4facfe' },
                areaStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: 'rgba(79,172,254,0.3)' },
                        { offset: 1, color: 'rgba(79,172,254,0.05)' }
                    ])
                }
            },
            {
                name: 'UV',
                type: 'line',
                data: uvs,
                smooth: true,
                lineStyle: { color: '#00e676', width: 2 },
                itemStyle: { color: '#00e676' },
                areaStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: 'rgba(0,230,118,0.3)' },
                        { offset: 1, color: 'rgba(0,230,118,0.05)' }
                    ])
                }
            }
        ]
    });
}

// ============================================================
// 3. 漏斗图
// ============================================================

async function updateFunnelChart() {
    const today = new Date().toISOString().split('T')[0];
    const data = await API.getFunnel(today);
    if (!data || data.length === 0) return;

    const funnelData = data.map(d => ({
        name: d.step_name,
        value: d.user_count
    }));

    chartFunnel.setOption({
        tooltip: {
            trigger: 'item',
            formatter: '{b}: {c} ({d}%)',
            backgroundColor: 'rgba(10,14,39,0.9)',
            borderColor: '#4facfe',
            textStyle: { color: '#e0e6ed' }
        },
        series: [{
            type: 'funnel',
            data: funnelData,
            gap: 2,
            label: {
                show: true,
                position: 'inside',
                formatter: '{b}\n{c}',
                color: '#fff',
                fontSize: 13
            },
            labelLine: { show: false },
            itemStyle: {
                borderWidth: 0,
                shadowBlur: 10,
                shadowColor: 'rgba(79,172,254,0.3)'
            },
            emphasis: {
                label: { fontSize: 16 }
            }
        }],
        color: ['#4facfe', '#00d2ff', '#00e676', '#ffab40']
    });
}

// ============================================================
// 4. 留存率图（柱状图 + 折线）
// ============================================================

async function updateRetentionChart() {
    const today = new Date().toISOString().split('T')[0];
    const data = await API.getRetention(today);
    if (!data || data.length === 0) return;

    const days = data.map(d => `第${d.ret_day}天`);
    const rates = data.map(d => d.retention_rate);
    const retained = data.map(d => d.retained_count);

    chartRetention.setOption({
        tooltip: {
            trigger: 'axis',
            backgroundColor: 'rgba(10,14,39,0.9)',
            borderColor: '#4facfe',
            textStyle: { color: '#e0e6ed' }
        },
        grid: DARK_THEME.grid,
        xAxis: {
            type: 'category',
            data: days,
            axisLabel: { color: '#8899aa' },
            axisLine: { lineStyle: { color: '#2a3550' } }
        },
        yAxis: [
            {
                type: 'value',
                name: '留存用户数',
                axisLabel: { color: '#8899aa' },
                splitLine: { lineStyle: { color: '#1a2540' } }
            },
            {
                type: 'value',
                name: '留存率 (%)',
                axisLabel: { color: '#8899aa', formatter: '{value}%' },
                splitLine: { show: false }
            }
        ],
        series: [
            {
                name: '留存用户',
                type: 'bar',
                data: retained,
                itemStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: '#4facfe' },
                        { offset: 1, color: '#1a6dd4' }
                    ]),
                    borderRadius: [6, 6, 0, 0]
                }
            },
            {
                name: '留存率',
                type: 'line',
                yAxisIndex: 1,
                data: rates,
                smooth: true,
                lineStyle: { color: '#ffab40', width: 3 },
                itemStyle: { color: '#ffab40' },
                symbol: 'circle',
                symbolSize: 8,
                label: {
                    show: true,
                    formatter: '{c}%',
                    color: '#ffab40',
                    fontSize: 12
                }
            }
        ]
    });
}

// ============================================================
// 5. AB 实验对比面板
// ============================================================

const chartAB = echarts.init(document.getElementById('chart-ab-experiment'));

async function updateABChart() {
    const today = new Date().toISOString().split('T')[0];
    const data = await API.getABExperiment('exp_001_home_rec_algo', today);
    if (!data || data.length === 0) return;

    // 按 metric_name 分组 { pv: {A: ..., B: ...}, uv: {...}, ... }
    const grouped = {};
    data.forEach(d => {
        if (!grouped[d.metric_name]) grouped[d.metric_name] = {};
        grouped[d.metric_name][d.variant] = d;
    });

    // 取 4 个核心指标做对比
    const metricsToShow = [
        { key: 'pv', label: 'PV' },
        { key: 'uv', label: 'UV' },
        { key: 'download_count', label: '下载量' },
        { key: 'download_conversion_rate', label: '下载转化率(%)' },
    ];

    const categories = metricsToShow.map(m => m.label);
    const aValues = metricsToShow.map(m =>
        grouped[m.key] && grouped[m.key]['A'] ? grouped[m.key]['A'].metric_value : 0
    );
    const bValues = metricsToShow.map(m =>
        grouped[m.key] && grouped[m.key]['B'] ? grouped[m.key]['B'].metric_value : 0
    );

    // 显著性标记
    const sigMarks = metricsToShow.map(m => {
        const b = grouped[m.key] && grouped[m.key]['B'] ? grouped[m.key]['B'] : null;
        if (b && b.is_significant) return '✅';
        if (b && b.p_value >= 0) return '❌';
        return '';
    });

    chartAB.setOption({
        tooltip: {
            trigger: 'axis',
            backgroundColor: 'rgba(10,14,39,0.9)',
            borderColor: '#4facfe',
            textStyle: { color: '#e0e6ed' },
            formatter: function(params) {
                let html = params[0].axisValue + '<br/>';
                params.forEach(p => {
                    const uplift = grouped[metricsToShow[p.dataIndex].key] &&
                                   grouped[metricsToShow[p.dataIndex].key]['B']
                        ? grouped[metricsToShow[p.dataIndex].key]['B'].uplift : 0;
                    html += `${p.marker} ${p.seriesName}: ${p.value.toLocaleString()}`;
                    if (p.seriesName === 'B组(实验组)' && uplift !== 0) {
                        html += ` <span style="color:${uplift > 0 ? '#00e676' : '#ff5252'}">(${uplift > 0 ? '+' : ''}${uplift}%)</span>`;
                    }
                    html += '<br/>';
                });
                return html;
            }
        },
        legend: {
            data: ['A组(对照组)', 'B组(实验组)'],
            textStyle: { color: '#8899aa' },
            top: 0
        },
        grid: { top: 40, right: 30, bottom: 40, left: 80 },
        xAxis: {
            type: 'category',
            data: categories,
            axisLabel: { color: '#b0c4de', fontSize: 12 },
            axisLine: { lineStyle: { color: '#2a3550' } },
            axisTick: { show: false }
        },
        yAxis: {
            type: 'value',
            axisLabel: { color: '#8899aa' },
            splitLine: { lineStyle: { color: '#1a2540' } }
        },
        series: [
            {
                name: 'A组(对照组)',
                type: 'bar',
                data: aValues,
                itemStyle: {
                    color: '#4facfe',
                    borderRadius: [4, 4, 0, 0]
                },
                barGap: '10%',
                label: {
                    show: true,
                    position: 'top',
                    color: '#8899aa',
                    fontSize: 10,
                    formatter: p => p.value >= 1000 ? (p.value / 1000).toFixed(1) + 'k' : p.value
                }
            },
            {
                name: 'B组(实验组)',
                type: 'bar',
                data: bValues,
                itemStyle: {
                    color: '#00e676',
                    borderRadius: [4, 4, 0, 0]
                },
                label: {
                    show: true,
                    position: 'top',
                    color: '#00e676',
                    fontSize: 10,
                    formatter: function(p) {
                        const mark = sigMarks[p.dataIndex];
                        const val = p.value >= 1000 ? (p.value / 1000).toFixed(1) + 'k' : p.value;
                        return mark ? val + ' ' + mark : val;
                    }
                }
            }
        ]
    });
}

// ============================================================
// 6. Top 下载排行（横向柱状图）
// ============================================================

async function updateTopDownloadsChart() {
    const data = await API.getTopDownloads(10);
    if (!data || data.length === 0) return;

    // 反转：横向柱状图从下到上排列（第一名在最上）
    const reversed = [...data].reverse();

    chartTopDownloads.setOption({
        tooltip: {
            trigger: 'axis',
            axisPointer: { type: 'shadow' },
            backgroundColor: 'rgba(10,14,39,0.9)',
            borderColor: '#4facfe',
            textStyle: { color: '#e0e6ed' }
        },
        grid: { top: 10, right: 40, bottom: 10, left: 80 },
        xAxis: {
            type: 'value',
            axisLabel: { color: '#8899aa' },
            splitLine: { lineStyle: { color: '#1a2540' } }
        },
        yAxis: {
            type: 'category',
            data: reversed.map(d => d.app_name),
            axisLabel: { color: '#b0c4de', fontSize: 13 },
            axisLine: { lineStyle: { color: '#2a3550' } }
        },
        series: [{
            type: 'bar',
            data: reversed.map(d => d.download_count),
            itemStyle: {
                color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                    { offset: 0, color: '#1a6dd4' },
                    { offset: 1, color: '#4facfe' }
                ]),
                borderRadius: [0, 6, 6, 0]
            },
            label: {
                show: true,
                position: 'right',
                color: '#b0c4de',
                fontSize: 12
            }
        }]
    });
}

// ============================================================
// 更新时钟 + 定期刷新
// ============================================================

function updateClock() {
    const now = new Date();
    document.getElementById('current-time').textContent =
        now.toLocaleString('zh-CN', {
            year: 'numeric', month: '2-digit', day: '2-digit',
            hour: '2-digit', minute: '2-digit', second: '2-digit'
        });
}

// ============================================================
// 启动大屏
// ============================================================

async function initDashboard() {
    console.log('🚀 数据大屏初始化...');

    // 首次加载
    await Promise.all([
        updateKPIs(),
        updateTrendChart(),
        updateFunnelChart(),
        updateRetentionChart(),
        updateABChart(),
        updateTopDownloadsChart()
    ]);

    updateClock();

    console.log('✅ 大屏初始化完成');

    // 定时刷新
    setInterval(updateClock, 1000);        // 时钟每秒
    setInterval(updateKPIs, 5000);         // KPI 每 5 秒
    setInterval(updateTrendChart, 60000);  // 趋势图每 1 分钟
    setInterval(updateABChart, 60000);     // AB 实验每 1 分钟
}

// ── 入口 ──
document.addEventListener('DOMContentLoaded', initDashboard);
