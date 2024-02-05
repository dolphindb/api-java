package com.xxdb.streaming.client;

import com.xxdb.streaming.client.TopicManager;
import org.junit.Test;

import java.lang.reflect.*;
import java.net.SocketException;
import java.util.Optional;

import static org.junit.Assert.*;
public class TopicManagerTest{
    @Test
    public void test_TopicManager_basic() throws SocketException {
        TopicManager tp = TopicManager.getInstance();
        TopicManager tp1 = TopicManager.getInstance();
        assertEquals(tp,tp1);
        assertFalse(tp.isTopicExists("Dolphindb"));
        tp1.addTopic("dolphindb");
        assertTrue(tp.isTopicExists("dolphindb"));
        assertNull(tp.getMessageQueue("dolphindb"));
        assertTrue(tp.addMessageQueue("dolphindb").isEmpty());
        assertNull(tp.addMessageQueue("oceanBase"));
        assertNull(tp.getSites("dolphindb"));
        assertEquals("dolphindb",tp1.getTopic("dolphindb"));
        tp.removeTopic("dolphindb");
        assertFalse(tp1.isTopicExists("dolphindb"));
        assertFalse(tp1.getAllTopic().isEmpty());
        assertNull(tp.getNameToIndex("dolphindb"));

    }

    @Test
    public void test_TopicManager_getAllTopic(){
        TopicManager tp = TopicManager.getInstance();
        tp.addTopic("dolphindb1");
       assertNull(tp.getNameToIndex("dolphindb1"));
       TopicManager.Utils utils = new TopicManager.Utils();
       assertEquals("dolphindb1", TopicManager.Utils.getSiteFromTopic("dolphindb1/"));
       tp.addTopic("OceanBase");
       tp.addTopic("kingBase");
       assertEquals("[OceanBase, kingBase, dolphindb1]",tp.getAllTopic().toString());
    }

}
