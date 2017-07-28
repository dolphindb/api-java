package com.xxdb.streaming.client;


import com.xxdb.streaming.client.datatransferobject.IMessage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class PollingClient extends AbstractClient{
    public PollingClient() {
        this(DEFAULT_HOST,DEFAULT_PORT);
    }

    public PollingClient(String localIP,int subscribePort) {
        super(localIP,subscribePort);
    }

    public TopicPoller subscribe(String host,int port,String tableName, long offset) throws IOException{
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host,port,tableName, offset);
        return new TopicPoller(queue);
    }
    // subscribe to host:port on tableName with offset set to position past the last element
    public TopicPoller subscribe(String host,int port,String tableName) throws IOException{
        return subscribe(host, port, tableName, -1);
    }

    public void unsubscribe(String host,int port ,String tableName) throws IOException {
        unsubscribeInternal(host, port, tableName);
    }
}
