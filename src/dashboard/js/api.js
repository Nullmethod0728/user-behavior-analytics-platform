/**
 * API 请求模块
 *
 * 从 Spring Boot API 拉取数据。
 * 如果 API 不可用（后端没启动），使用静态演示数据展示大屏效果。
 */
const API = (() => {

    const BASE_URL = 'http://localhost:8080/api/v1';
    const DEMO_MODE = true;  // 设为 false 则从真实 API 拉数据

    /**
     * 通用 GET 请求
     */
    async function get(path) {
        if (DEMO_MODE) {
            console.log(`[API Demo] GET ${path} → 返回模拟数据`);
            return getDemoData(path);
        }

        try {
            const response = await fetch(`${BASE_URL}${path}`);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const json = await response.json();
            return json.data;
        } catch (err) {
            console.warn(`[API] 请求失败: ${path}`, err.message);
            return null;
        }
    }

    /**
     * 实时指标（每 5 秒轮询）
     */
    async function getRealtimeMetrics() {
        if (DEMO_MODE) {
            const pv = Math.floor(Math.random() * 50000 + 80000);
            const uv = Math.floor(pv * (0.3 + Math.random() * 0.15));
            return { pv, uv, redis_connected: true };
        }
        return await get('/realtime/metrics');
    }

    /**
     * 天级汇总（趋势图用）
     */
    async function getDailyMetrics(from, to) {
        return await get(`/analytics/daily?from=${from}&to=${to}`);
    }

    /**
     * 留存数据
     */
    async function getRetention(date) {
        return await get(`/analytics/retention?date=${date}`);
    }

    /**
     * 漏斗数据
     */
    async function getFunnel(date) {
        return await get(`/analytics/funnel?date=${date}`);
    }

    /**
     * Top 下载排行
     */
    async function getTopDownloads(limit) {
        return await get(`/analytics/top-downloads?limit=${limit}`);
    }

    /**
     * AB 实验数据
     */
    async function getABExperiment(experimentId, date) {
        if (DEMO_MODE) return generateDemoABExperiment();
        return await get(`/ab/experiment?experimentId=${experimentId}&date=${date}`);
    }

    // ============================================================
    // 演示数据（API 没启动时的 fallback）
    // ============================================================

    function getDemoData(path) {
        if (path.includes('daily')) return generateDemoDaily();
        if (path.includes('retention')) return generateDemoRetention();
        if (path.includes('funnel')) return generateDemoFunnel();
        if (path.includes('top-downloads')) return generateDemoTopDownloads();
        return null;
    }

    /** 生成最近 7 天的模拟日汇总数据 */
    function generateDemoDaily() {
        const data = [];
        const today = new Date();
        for (let i = 6; i >= 0; i--) {
            const date = new Date(today);
            date.setDate(date.getDate() - i);
            const dateStr = date.toISOString().split('T')[0];
            const basePv = 80000 + Math.floor(Math.random() * 40000);
            data.push({
                stat_date: dateStr,
                pv: basePv,
                uv: Math.floor(basePv * 0.35),
                download_count: Math.floor(basePv * 0.05),
                search_count: Math.floor(basePv * 0.08)
            });
        }
        return data;
    }

    /** 模拟留存数据 */
    function generateDemoRetention() {
        return [
            { ret_day: 1, new_user_count: 5000, retained_count: 2000, retention_rate: 40.0 },
            { ret_day: 3, new_user_count: 5000, retained_count: 1250, retention_rate: 25.0 },
            { ret_day: 7, new_user_count: 5000, retained_count: 750, retention_rate: 15.0 },
            { ret_day: 30, new_user_count: 5000, retained_count: 400, retention_rate: 8.0 },
        ];
    }

    /** 模拟漏斗数据 */
    function generateDemoFunnel() {
        return [
            { step_order: 1, step_name: '曝光', user_count: 100000, step_rate: 100, overall_rate: 100 },
            { step_order: 2, step_name: '点击', user_count: 20000, step_rate: 20, overall_rate: 20 },
            { step_order: 3, step_name: '下载', user_count: 5000, step_rate: 25, overall_rate: 5 },
            { step_order: 4, step_name: '安装', user_count: 3500, step_rate: 70, overall_rate: 3.5 },
        ];
    }

    /** 模拟 Top 下载 */
    function generateDemoTopDownloads() {
        const apps = ['微信', '抖音', '拼多多', '淘宝', '京东', '美团', '王者荣耀', '原神', 'B站', '知乎'];
        return apps.map((name, i) => ({
            app_name: name,
            download_count: Math.floor((apps.length - i) * 800 + Math.random() * 400)
        }));
    }

    /** 模拟 AB 实验数据 */
    function generateDemoABExperiment() {
        // 模拟首页推荐算法实验：A组(热度排序) vs B组(兴趣排序)
        const expId = 'exp_001_home_rec_algo';
        const aPv = 45000 + Math.floor(Math.random() * 5000);
        const bPv = 48000 + Math.floor(Math.random() * 5000);
        const aUv = Math.floor(aPv * 0.35);
        const bUv = Math.floor(bPv * 0.38);
        const aDownload = Math.floor(aPv * 0.042);
        const bDownload = Math.floor(bPv * 0.053);
        const aConv = (aDownload / aUv * 100);
        const bConv = (bDownload / bUv * 100);

        return [
            // A 组（对照组）
            { experiment_id: expId, variant: 'A', metric_name: 'pv', metric_value: aPv, metric_unit: '次', user_count: aUv, p_value: -1, is_significant: 0, uplift: 0 },
            { experiment_id: expId, variant: 'A', metric_name: 'uv', metric_value: aUv, metric_unit: '人', user_count: aUv, p_value: -1, is_significant: 0, uplift: 0 },
            { experiment_id: expId, variant: 'A', metric_name: 'download_count', metric_value: aDownload, metric_unit: '次', user_count: aUv, p_value: -1, is_significant: 0, uplift: 0 },
            { experiment_id: expId, variant: 'A', metric_name: 'download_conversion_rate', metric_value: parseFloat(aConv.toFixed(2)), metric_unit: '%', user_count: aUv, p_value: 0.032, is_significant: 1, uplift: parseFloat((((bConv - aConv) / aConv) * 100).toFixed(2)) },
            { experiment_id: expId, variant: 'A', metric_name: 'avg_duration', metric_value: 12500 + Math.floor(Math.random() * 2000), metric_unit: 'ms', user_count: aUv, p_value: 0.18, is_significant: 0, uplift: 0 },
            { experiment_id: expId, variant: 'A', metric_name: 'search_count', metric_value: 3800 + Math.floor(Math.random() * 500), metric_unit: '次', user_count: aUv, p_value: 0.55, is_significant: 0, uplift: 0 },
            // B 组（实验组）
            { experiment_id: expId, variant: 'B', metric_name: 'pv', metric_value: bPv, metric_unit: '次', user_count: bUv, p_value: -1, is_significant: 0, uplift: parseFloat((((bPv - aPv) / aPv) * 100).toFixed(2)) },
            { experiment_id: expId, variant: 'B', metric_name: 'uv', metric_value: bUv, metric_unit: '人', user_count: bUv, p_value: -1, is_significant: 0, uplift: parseFloat((((bUv - aUv) / aUv) * 100).toFixed(2)) },
            { experiment_id: expId, variant: 'B', metric_name: 'download_count', metric_value: bDownload, metric_unit: '次', user_count: bUv, p_value: -1, is_significant: 0, uplift: parseFloat((((bDownload - aDownload) / aDownload) * 100).toFixed(2)) },
            { experiment_id: expId, variant: 'B', metric_name: 'download_conversion_rate', metric_value: parseFloat(bConv.toFixed(2)), metric_unit: '%', user_count: bUv, p_value: 0.032, is_significant: 1, uplift: parseFloat((((bConv - aConv) / aConv) * 100).toFixed(2)) },
            { experiment_id: expId, variant: 'B', metric_name: 'avg_duration', metric_value: 13800 + Math.floor(Math.random() * 2000), metric_unit: 'ms', user_count: bUv, p_value: 0.18, is_significant: 0, uplift: parseFloat((((13800 - 12500) / 12500) * 100).toFixed(2)) },
            { experiment_id: expId, variant: 'B', metric_name: 'search_count', metric_value: 3900 + Math.floor(Math.random() * 500), metric_unit: '次', user_count: bUv, p_value: 0.55, is_significant: 0, uplift: parseFloat((((3900 - 3800) / 3800) * 100).toFixed(2)) },
        ];
    }

    // ── 公开接口 ──
    return {
        getRealtimeMetrics,
        getDailyMetrics,
        getRetention,
        getFunnel,
        getTopDownloads,
        getABExperiment
    };

})();
