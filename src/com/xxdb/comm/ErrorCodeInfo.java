package com.xxdb.comm;

import com.xxdb.multithreadtablewriter.MultithreadTableWriter;

public class ErrorCodeInfo {
    public enum Code {
        EC_None,
        EC_InvalidObject,
        EC_InvalidParameter,
        EC_InvalidTable,
        EC_InvalidColumnType,
        EC_Server,
        EC_UserBreak,
    };
    public ErrorCodeInfo(int code, String info){
        set(code,info);
    }
    public ErrorCodeInfo(Code code, String info){
        set(code,info);
    }
    public ErrorCodeInfo(ErrorCodeInfo src){
        set(src.errorCode,src.errorInfo);
    }
    public void set(ErrorCodeInfo errorCodeInfo){
        set(errorCodeInfo.errorCode,errorCodeInfo.errorInfo);
    }
    public void set(int code, String info){
        this.errorCode=code;
        this.errorInfo=info;
    }
    public void set(Code code, String info){
        set(code.ordinal(),info);
    }
    public int errorCode;
    public String errorInfo;
}
