package com.xxdb.streaming.client;


import com.xxdb.streaming.client.IMessage;

import java.io.IOException;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class PollingClient extends AbstractClient{
	TopicPoller topicPoller = null;

    public PollingClient(int subscribePort) throws SocketException {
        super(subscribePort);
    }
    
    @Override
    protected void doReconnect(Site site) {
    	while (true) {
    		try {
    			Thread.sleep(5000);
    			BlockingQueue<List<IMessage>> queue = subscribeInternal(site.host, site.port, site.tableName, site.actionName, site.msgId + 1, true);
    			topicPoller.setQueue(queue);
    			return;
    		} catch (Exception ex) {
				System.out.println("Unable to subscribe table. Will try again after 5 seconds.");
    		}
    	}
    }
    
    public TopicPoller subscribe(String host,int port,String tableName,String actionName,long offset,boolean reconnect) throws IOException{
    	BlockingQueue<List<IMessage>> queue = subscribeInternal(host,port,tableName,actionName,offset,reconnect);
    	topicPoller = new TopicPoller(queue);
    	return topicPoller;
	}

    public TopicPoller subscribe(String host,int port,String tableName,String actionName,long offset) throws IOException{
        return subscribe(host,port,tableName,actionName,offset,false);
    }

    public TopicPoller subscribe(String host,int port,String tableName,long offset) throws IOException{
        return subscribe(host,port,tableName,DEFAULT_ACTION_NAME,offset);
    }
    
    public TopicPoller subscribe(String host,int port,String tableName,long offset,boolean reconnect) throws IOException{
        return subscribe(host,port,tableName,DEFAULT_ACTION_NAME,offset,reconnect);
    }

	public TopicPoller subscribe(String host,int port,String tableName) throws IOException{
        return subscribe(host, port, tableName, -1);
    }
    
    public TopicPoller subscribe(String host,int port,String tableName,boolean reconnect) throws IOException{
        return subscribe(host, port, tableName, -1, reconnect);
    }

    public TopicPoller subscribe(String host,int port,String tableName,String actionName) throws IOException{
        return subscribe(host, port, tableName,actionName, -1);
    }
    
    public TopicPoller subscribe(String host,int port,String tableName,String actionName,boolean reconnect) throws IOException{
        return subscribe(host, port, tableName,actionName, -1, reconnect);
    }
    
    public void unsubscribe(String host,int port,String tableName,String actionName) throws IOException {
        unsubscribeInternal(host, port, tableName,actionName);
    }
    
    public void unsubscribe(String host,int port ,String tableName) throws IOException {
        unsubscribeInternal(host, port, tableName,DEFAULT_ACTION_NAME);
    }
}
