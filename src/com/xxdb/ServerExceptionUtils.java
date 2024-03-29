package com.xxdb;

public class ServerExceptionUtils {
    public static boolean isNotLogin(String exMsg) {
        return exMsg.indexOf("<NotAuthenticated>") >= 0;
    }

    public static boolean isNotLeader(String exMsg) {
        if (exMsg == null)
            return false;
        return exMsg.indexOf("<NotLeader>") >= 0;
    }

    public static boolean isDataNodeNotAvailable(String exMsg) {
        if (exMsg == null)
            return false;
        return exMsg.indexOf("<DataNodeNotAvail>") >= 0;
    }

    public static String newLeader(String exMsg) {
        return exMsg.substring(11);
    }
}
