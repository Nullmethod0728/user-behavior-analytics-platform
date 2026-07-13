package com.analytics.vo;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * 统一 API 响应格式
 *
 * 所有接口返回这个结构，前端统一处理：
 * {
 *   "code": 200,
 *   "message": "success",
 *   "data": { ... }
 * }
 *
 * @param <T> data 的类型
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Result<T> {

    private int code;
    private String message;
    private T data;

    private Result(int code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    /** 成功 */
    public static <T> Result<T> ok(T data) {
        return new Result<>(200, "success", data);
    }

    /** 失败 */
    public static <T> Result<T> fail(int code, String message) {
        return new Result<>(code, message, null);
    }

    // ── getter/setter ──
    public int getCode() { return code; }
    public void setCode(int code) { this.code = code; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public T getData() { return data; }
    public void setData(T data) { this.data = data; }
}
