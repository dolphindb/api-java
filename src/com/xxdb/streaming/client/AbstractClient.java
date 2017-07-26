package com.xxdb.streaming.client;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import com.xxdb.streaming.DBConnection;
import com.xxdb.streaming.client.datatransferobject.IMessage;
import com.xxdb.streaming.data.BasicInt;
import com.xxdb.streaming.data.BasicLong;
import com.xxdb.streaming.data.BasicString;
import com.xxdb.streaming.data.Entity;

abstract class AbstractClient implements MessageDispatcher{
	
	protected static final int DEFAULT_PORT = 8849;

	protected int listeningPort;

	protected QueueManager queueManager = new QueueManager();
	protected HashMap<String, List<IMessage>> messageCache = new HashMap<>();
	protected HashMap<String, String> tableName2Topic = new HashMap<>();
	public AbstractClient(){
		this(DEFAULT_PORT);
	}
	public AbstractClient(int subscribePort){
		this.listeningPort = subscribePort;
		Daemon daemon = new Daemon(subscribePort, this);
		Thread pThread = new Thread(daemon);
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
			} catch (InterruptedException e) {
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
	// establish a connection between dolphindb server
	// return a queue exclusively for the table.
	protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port, String tableName, long offset) throws IOException,RuntimeException {

		Entity re;
		DBConnection dbConn = new DBConnection();
		String topic = "";

		dbConn.connect(host, port);

		List<Entity> params = new ArrayList<Entity>();

		params.add(new BasicString(tableName));
		re = dbConn.run("getSubscriptionTopic", params);
		topic = re.getString();
		BlockingQueue<List<IMessage>> queue = queueManager.addQueue(topic);
		params.clear();
		tableName2Topic.put(tableName, topic);
		params.add(new BasicString(GetLocalIP()));
		params.add(new BasicInt(this.listeningPort));
		params.add(new BasicString(tableName));
		if (offset != -1)
		params.add(new BasicLong(offset));
		dbConn.run("publishTable", params);
		dbConn.close();
		return queue;
	}
	
	
	protected void unsubscribeInternal(String host,int port ,String tableName) throws IOException {
		
		Entity re;
		
		DBConnection dbConn = new DBConnection();
		
		dbConn.connect(host, port);
		
		List<Entity> params = new ArrayList<Entity>();
		params.add(new BasicString(tableName));
		re = dbConn.run("stopSubscribeTable", params);
		dbConn.close();
		
		return;
		
	}
	
	private String GetLocalIP(){
		Enumeration allNetInterfaces = null;
		String localIp = "127.0.0.1";
		try {
			allNetInterfaces = NetworkInterface.getNetworkInterfaces();
		} catch (SocketException e) {
			e.printStackTrace();
		}
		InetAddress ip = null;
		while (allNetInterfaces.hasMoreElements())		{
			NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();

			Enumeration addresses = netInterface.getInetAddresses();
			while (addresses.hasMoreElements()){
				ip = (InetAddress) addresses.nextElement();
				if (ip != null && ip instanceof Inet4Address){
					localIp = ip.getHostAddress();
				} 
			}
		}
		//System.out.println(localIp);
		return localIp;
	}
}
