package com.deepexi.tarimdb.util;

/**
 * Status
 *
 */
public enum Status {
    
    // for common (0~99)
     OK                 (0, "OK")
    ,PARAM_ERROR        (1, "parameter error")
    ,TIMEOUT_ERROR      (2, "timeout")
    ,SERVER_START_FAILED(3, "server start failed")
    ,NULL_POINTER       (4, "null pointer")

    ,UNKNOWN_ERROR      (99, "unknown error")

    // for API (100~199)
    ,DISTRIBUTION_ERROR (100, "data distribution error")

    // for Resource Manager (200~299)
    ,MASTER_SLOT_NOT_FOUND (200, "master slot not found")

    //Storage Unit (500~599)
    ,KEY_ENCODE_ERROR   (500, "key encode error")

    // for replica manager (700~799)

    ,ERROR_END          (2000, "error end");

    private int code;
    private String msg;

    private Status(int code){
        this.code = code;
        //this.msg = getMsg(code);
    }
    private Status(int code, String msg){
        this.code = code;
        this.msg = msg;
    }
    public int getCode(){
         return code;
    }
    public String getMsg(){
         return msg;
    }
}
