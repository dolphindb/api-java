package com.xxdb.client;


import com.xxdb.client.datatransferobject.IMessage;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class PollingClient extends AbstractClient{
    public PollingClient() {
        this(DEFAULT_PORT);
    }

    public PollingClient(int subscribePort) {
        super(subscribePort);
    }

    public TopicPoller subscribe(String host,int port,String tableName, long offset) throws IOException{
        BlockingQueue<IMessage> queue = subscribeTo(host,port,tableName, offset);
        return new TopicPoller(queue);
    }
    // subscribe to host:port on tableName with offset set to position past the last element
    public TopicPoller subscribe(String host,int port,String tableName) throws IOException{
        return subscribe(host, port, tableName, -1);
    }
}
