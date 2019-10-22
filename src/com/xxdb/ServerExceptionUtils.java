package com.xxdb;

public class ServerExceptionUtils {
    public static Boolean isNotLogin(String exMsg)
    {
        return exMsg.indexOf("<NotAuthenticated>")>=0;
    }
}
