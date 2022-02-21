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
        EC_NullValue,
    };
    public ErrorCodeInfo(){
        set(0,"");
    }
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
    @Override
    public String toString(){
        StringBuilder sb=new StringBuilder();
        sb.append("code=").append(errorCode).append(" info=").append(errorInfo);
        return sb.toString();
    }
    public void set(Code code, String info){
        set(code.ordinal(),info);
    }
    public int errorCode;
    public String errorInfo;
}
