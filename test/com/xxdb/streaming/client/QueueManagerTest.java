package com.xxdb.streaming.client;

import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.util.List;

public class QueueManagerTest {
    @Test
    public void test_QueueManager_addQueue() throws IOException {
        QueueManager queueManager = new QueueManager();
        queueManager.addQueue("dolphindb");
        List<String> re = queueManager.getAllTopic();
        System.out.println(re.toString());
        String re1 = null;
        try{
            queueManager.addQueue("dolphindb");
        }catch(Exception ex){
            re1 = ex.toString();
        }
        Assert.assertEquals("java.lang.RuntimeException: Topic dolphindb already subscribed",re1);
    }
    @Test
    public void test_QueueManager_getAllTopic() throws IOException {
        QueueManager queueManager = new QueueManager();
        queueManager.addQueue("dolphindb");
        List<String> re = queueManager.getAllTopic();
        Assert.assertEquals("[dolphindb]",re.toString());
    }
}
