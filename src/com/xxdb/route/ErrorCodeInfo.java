package com.xxdb.route;

public class  ErrorCodeInfo{
    public enum Code{
        EC_None,
        EC_InvalidObject,
        EC_InvalidParameter,
        EC_InvalidTable,
        EC_InvalidColumnType,
        EC_Server,
        EC_UserBreak,
    };
    public ErrorCodeInfo() {
        errorCode = 0;
    }
    public ErrorCodeInfo(int code,String info) {
        set(code,info);
    }
    public ErrorCodeInfo(Code code,String info) {
        set(code,info);
    }
    public void set(Code code,String info){
        set(code.ordinal(),info);
    }
    public void set(int code,String info){
        errorCode = code;
        errorInfo = info;
    }
    public void set(ErrorCodeInfo psrc){
        set(psrc.errorCode,psrc.errorInfo);
    }
    public int errorCode;
    public String errorInfo;
};
