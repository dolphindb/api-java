package com.xxdb.streaming.client;

import java.io.IOException;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.BlockingQueue;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicLong;
import com.xxdb.data.BasicString;
import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.Entity;
import com.xxdb.streaming.client.IMessage;

abstract class AbstractClient implements MessageDispatcher{
	protected static final int DEFAULT_PORT = 8849;
	protected static final String DEFAULT_HOST = "localhost";
	protected static final String DEFAULT_ACTION_NAME = "javaStreamingApi";
	protected int listeningPort;
	protected QueueManager queueManager = new QueueManager();
	protected HashMap<String, List<IMessage>> messageCache = new HashMap<>();
	protected HashMap<String, String> tableName2Topic = new HashMap<>();
	protected HashMap<String, Boolean> hostEndian = new HashMap<>();
	protected Thread pThread ; 
	public AbstractClient() throws SocketException {
		this(DEFAULT_PORT);
	}
	public AbstractClient(int subscribePort) throws SocketException {
		this.listeningPort = subscribePort;
		Daemon daemon = new Daemon(subscribePort, this);
		pThread = new Thread(daemon);
		pThread.start();
	}

	private void addMessageToCache(IMessage msg) {
		String topic = msg.getTopic();
		List<IMessage> cache = messageCache.get(topic);
		if (cache == null) {
			cache = new ArrayList<>();
			messageCache.put(msg.getTopic(), cache);
		}
		cache.add(msg);
	}
	
	private void flushToQueue() {

		Set<String> keySet = messageCache.keySet();
		for(String topic : keySet) {
			try {
				queueManager.getQueue(topic).put(messageCache.get(topic));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		messageCache.clear();
	}

	public void dispatch(IMessage msg) {
		BlockingQueue<List<IMessage>> queue = queueManager.getQueue(msg.getTopic());
		try {
			queue.put(Arrays.asList(msg));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void batchDispatch(List<IMessage> messags) {
		for (int i = 0; i < messags.size(); ++i) {
			addMessageToCache(messags.get(i));
		}
		flushToQueue();
	}
	
	public boolean isRemoteLittleEndian(String host){
		if(hostEndian.containsKey(host)){
			return hostEndian.get(host);
		}
		else
			return false; 
	}
	
	protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port, String tableName,String actionName, long offset) throws IOException,RuntimeException {
		Entity re;
		String topic = "";
		
		DBConnection dbConn = new DBConnection();
		dbConn.connect(host, port);
		String localIP = dbConn.getLocalAddress().getHostAddress();
		
		if(!hostEndian.containsKey(host)){
			hostEndian.put(host, dbConn.getRemoteLittleEndian());
		}
		
		List<Entity> params = new ArrayList<Entity>();
		params.add(new BasicString(tableName));
		params.add(new BasicString(actionName));
		re = dbConn.run("getSubscriptionTopic", params);
		topic = ((BasicAnyVector)re).getEntity(0).getString();
		BlockingQueue<List<IMessage>> queue = queueManager.addQueue(topic);
		params.clear();
		
		tableName2Topic.put(host + ":" + port + ":" + tableName, topic);
		
		params.add(new BasicString(localIP));
		params.add(new BasicInt(this.listeningPort));
		params.add(new BasicString(tableName));
		params.add(new BasicString(actionName));
		if (offset != -1)
			params.add(new BasicLong(offset));
		re = dbConn.run("publishTable", params);
		
		dbConn.close();
		return queue;
	}
	
	protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port, String tableName, long offset) throws IOException,RuntimeException {
		return subscribeInternal(host,port,tableName,DEFAULT_ACTION_NAME,offset);
	}
	
	protected void unsubscribeInternal(String host,int port ,String tableName,String actionName) throws IOException {
		DBConnection dbConn = new DBConnection();
		dbConn.connect(host, port);
		String localIP = dbConn.getLocalAddress().getHostAddress();
		List<Entity> params = new ArrayList<Entity>();
		params.add(new BasicString(localIP));
		params.add(new BasicInt(this.listeningPort));
		params.add(new BasicString(tableName));
		params.add(new BasicString(actionName));
		dbConn.run("stopPublishTable", params);
		dbConn.close();
		return;
	}

	protected void unsubscribeInternal(String host,int port ,String tableName) throws IOException {
		unsubscribeInternal(host, port, tableName, DEFAULT_ACTION_NAME);
	}
}
