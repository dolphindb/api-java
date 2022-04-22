package com.xxdb.comm;

public class ErrorCodeInfo {
    public enum Code {
        EC_None(0),
        EC_InvalidObject(1),
        EC_InvalidParameter(2),
        EC_InvalidTable(3),
        EC_InvalidColumnType(4),
        EC_Server(5),
        EC_UserBreak(6),
        EC_DestroyedObject(7),
        EC_OTHER(8);

        public int value;
        Code(int value){
            this.value = value;
        }
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
        set(formatApiCode(code), info);
    }
    @Override
    public String toString(){
        StringBuilder sb=new StringBuilder();
        sb.append("code=").append(errorCode).append(" info=").append(errorInfo);
        return sb.toString();
    }
    public void set(Code code, String info){
        set(formatApiCode(code.value),info);
    }

    public void set(String code, String info){
        this.errorCode = code;
        this.errorInfo = info;
    }

    public boolean hasError(){
        return errorCode.isEmpty() == false;
    }

    public static String formatApiCode(int code){
        if (code != Code.EC_None.value){
            return "A" + code;
        }else
            return "";
    }
    public String errorCode;
    public String errorInfo;
}
