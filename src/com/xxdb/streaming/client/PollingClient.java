package com.xxdb.streaming.client;


import com.xxdb.streaming.client.IMessage;

import java.io.IOException;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class PollingClient extends AbstractClient{
    public PollingClient() throws SocketException {
        this(DEFAULT_PORT);
    }

    public PollingClient(int subscribePort) throws SocketException {
        super(subscribePort);
    }

    public TopicPoller subscribe(String host,int port,String tableName, long offset) throws IOException{
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host,port,tableName, offset);
        return new TopicPoller(queue);
    }

    public TopicPoller subscribe(String host,int port,String tableName) throws IOException{
        return subscribe(host, port, tableName, -1);
    }

    public void unsubscribe(String host,int port ,String tableName) throws IOException {
        unsubscribeInternal(host, port, tableName);
    }
}
