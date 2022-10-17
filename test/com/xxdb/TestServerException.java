package com.xxdb;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestServerException {
    @Test
    public void test_isNotLogin(){
        String msg1 = "<NotAuthenticated>abc";
        String msg2 = "msg2";
        assertTrue(ServerExceptionUtils.isNotLogin(msg1));
        assertFalse(ServerExceptionUtils.isNotLogin(msg2));
    }

    @Test
    public void test_isNotLeader(){
        String msg1 = "<NotLeader>abc";
        String msg2 = "msg2";
        String msg3 = null;
        assertTrue(ServerExceptionUtils.isNotLeader(msg1));
        assertFalse(ServerExceptionUtils.isNotLeader(msg2));
        assertFalse(ServerExceptionUtils.isNotLeader(msg3));
    }

    @Test
    public void test_isDataNodeNotAvailable(){
        String msg1 = "<DataNodeNotAvail>abc";
        String msg2 = "msg2";
        String msg3 = null;
        assertTrue(ServerExceptionUtils.isDataNodeNotAvailable(msg1));
        assertFalse(ServerExceptionUtils.isDataNodeNotAvailable(msg2));
        assertFalse(ServerExceptionUtils.isDataNodeNotAvailable(msg3));
    }

    @Test
    public void test_newLeader(){
        String msg = "<NotLeader>192.168.1.116:18999";
        assertEquals("192.168.1.116:18999", ServerExceptionUtils.newLeader(msg));
    }
}
