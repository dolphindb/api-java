package com.xxdb.streaming.client;


import com.xxdb.streaming.client.IMessage;

import java.io.IOException;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class PollingClient extends AbstractClient{

    public PollingClient(int subscribePort) throws SocketException {
        super(subscribePort);
    }
    
    public TopicPoller subscribe(String host,int port,String tableName,String actionName, long offset) throws IOException{
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host,port,tableName,actionName, offset);
        return new TopicPoller(queue);
    }

    public TopicPoller subscribe(String host,int port,String tableName,long offset) throws IOException{
        return subscribe(host,port,tableName,DEFAULT_ACTION_NAME,offset);
    }
    
    public TopicPoller subscribe(String host,int port,String tableName) throws IOException{
        return subscribe(host, port, tableName, -1);
    }

    public TopicPoller subscribe(String host,int port,String tableName,String actionName) throws IOException{
        return subscribe(host, port, tableName,actionName, -1);
    }
    
    public void unsubscribe(String host,int port ,String tableName,String actionName) throws IOException {
        unsubscribeInternal(host, port, tableName,actionName);
    }
    
    public void unsubscribe(String host,int port ,String tableName) throws IOException {
        unsubscribeInternal(host, port, tableName,DEFAULT_ACTION_NAME);
    }
}
