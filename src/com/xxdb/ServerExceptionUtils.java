package com.xxdb;

public class ServerExceptionUtils {
    public static Boolean isNotLogin(String exMsg)
    {
        return exMsg.indexOf("<NotAuthenticated>")>=0;
    }
    
    public static Boolean isNotLeader(String exMsg)
    {
        return exMsg.indexOf("<NotLeader>")>=0;
    }
    
    public static String newLeader(String exMsg)
    {
    	return exMsg.substring(11);
    }
}
