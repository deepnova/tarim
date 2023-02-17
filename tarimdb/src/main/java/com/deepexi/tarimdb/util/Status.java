package com.deepexi.tarimdb.util;

/**
 * Status
 *
 */
public enum Status {
    OK(0, "OK"),
    PARAM_ERROR(1, "parameter error"),
    TIMEOUT_ERROR(2, "timeout"),
    SERVER_START_FAILED(3, "server start failed"),
    NULL_POINTER(4, "null pointer"),

    ERROR_END(2000, "error end");

    private int code;
    private String msg;

    Status(int code){
        this.code = code;
        //this.msg = getMsg(code);
    }
    private Status(int code, String msg){
        this.code = code;
        this.msg = msg;
    }
}
