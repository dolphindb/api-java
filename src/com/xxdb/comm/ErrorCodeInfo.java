package com.xxdb.comm;

public class ErrorCodeInfo {

    private String errorCode;
    private String errorInfo;

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

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorInfo() {
        return errorInfo;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public void setErrorInfo(String errorInfo) {
        this.errorInfo = errorInfo;
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

    public void clearError(){
        errorCode = "";
        errorInfo = "";
    }

    public boolean hasError(){
        return errorCode.isEmpty() == false;
    }

    public boolean succeed(){
        return errorCode.isEmpty() == true;
    }

    public static String formatApiCode(int code){
        if (code != Code.EC_None.value){
            return "A" + code;
        }else
            return "";
    }
}
