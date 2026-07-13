package com.analytics.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 用户行为事件 — Java 版数据模型
 *
 * 字段与 Python 端的 models.py 完全对齐，
 * 同时也对应 ClickHouse ods_user_behavior_log 表结构。
 *
 * 实现 Serializable 是因为 Flink 需要在网络上传输这些对象。
 * JsonIgnoreProperties 让 Jackson 反序列化时忽略 JSON 中多出来的字段。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserBehaviorEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    // ── 事件标识 ──
    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("event_type")
    private String eventType;

    // ── 用户标识 ──
    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("device_id")
    private String deviceId;

    // ── 时间 ──
    @JsonProperty("event_time")
    private String eventTime;       // Kafka 里是字符串 "2026-07-10 14:30:00"

    @JsonProperty("server_time")
    private String serverTime;

    // ── 设备信息 ──
    @JsonProperty("device_model")
    private String deviceModel;

    @JsonProperty("os_version")
    private String osVersion;

    @JsonProperty("app_version")
    private String appVersion;

    // ── 会话 ──
    @JsonProperty("session_id")
    private String sessionId;

    // ── 页面 ──
    private String page;

    // ── 位置 ──
    private String ip;
    private String city;

    // ── 行为 ──
    private int duration;           // 页面停留时长(毫秒)

    // ── 扩展属性 (JSON 字符串，在 Flink 里按需解析) ──
    private String properties;

    // ── 数据来源 ──
    private String source;

    // ============================================================
    // 构造方法
    // ============================================================

    /** 空构造方法，JSON 反序列化需要 */
    public UserBehaviorEvent() {
    }

    // ============================================================
    // Getter & Setter（Jackson 通过 getter 来序列化/反序列化）
    // ============================================================

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }

    public String getEventTime() { return eventTime; }
    public void setEventTime(String eventTime) { this.eventTime = eventTime; }

    public String getServerTime() { return serverTime; }
    public void setServerTime(String serverTime) { this.serverTime = serverTime; }

    public String getDeviceModel() { return deviceModel; }
    public void setDeviceModel(String deviceModel) { this.deviceModel = deviceModel; }

    public String getOsVersion() { return osVersion; }
    public void setOsVersion(String osVersion) { this.osVersion = osVersion; }

    public String getAppVersion() { return appVersion; }
    public void setAppVersion(String appVersion) { this.appVersion = appVersion; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public String getPage() { return page; }
    public void setPage(String page) { this.page = page; }

    public String getIp() { return ip; }
    public void setIp(String ip) { this.ip = ip; }

    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public int getDuration() { return duration; }
    public void setDuration(int duration) { this.duration = duration; }

    public String getProperties() { return properties; }
    public void setProperties(String properties) { this.properties = properties; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    @Override
    public String toString() {
        return "Event{" +
                "type='" + eventType + '\'' +
                ", user='" + userId + '\'' +
                ", page='" + page + '\'' +
                '}';
    }
}
