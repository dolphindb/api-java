package com.xxdb.streaming.client;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.BlockingQueue;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicLong;
import com.xxdb.data.BasicString;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.Entity;
import com.xxdb.streaming.client.datatransferobject.IMessage;

abstract class AbstractClient implements MessageDispatcher{
	
	protected static final int DEFAULT_PORT = 8849;
	protected static final String DEFAULT_HOST = "localhost";
	protected int listeningPort;
	protected String localIP;
	protected QueueManager queueManager = new QueueManager();
	protected HashMap<String, List<IMessage>> messageCache = new HashMap<>();
	protected HashMap<String, String> tableName2Topic = new HashMap<>();
	protected HashMap<String, Boolean> hostEndian = new HashMap<>();
	public AbstractClient() throws SocketException{
		this(DEFAULT_PORT);
	}
	public AbstractClient(int subscribePort) throws SocketException{
		this.listeningPort = subscribePort;
		this.localIP = this.GetLocalIP();
		Daemon daemon = new Daemon(subscribePort, this);
		Thread pThread = new Thread(daemon);
		pThread.start();
	}

	private void addMessageToCache(IMessage msg) {
		String topic = msg.getTopic();
		List<IMessage> cache = messageCache.get(topic);
		//if (!msg.getTopic().equals("rh8904_trades1"))
		//	assert(msg.getTopic() == "rh8904_trades1");
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
			return false; //default bigEndian
	}
	
	
	// establish a connection between dolphindb server
	// return a queue exclusively for the table.
	protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port, String tableName, long offset) throws IOException,RuntimeException {

		Entity re;
		DBConnection dbConn = new DBConnection();
		String topic = "";

		dbConn.connect(host, port);
		
		if(!hostEndian.containsKey(host)){
			hostEndian.put(host, dbConn.getRemoteLittleEndian());
		}
		
		List<Entity> params = new ArrayList<Entity>();

		params.add(new BasicString(tableName));
		re = dbConn.run("getSubscriptionTopic", params);
		topic = re.getString();
		BlockingQueue<List<IMessage>> queue = queueManager.addQueue(topic);
		params.clear();
		tableName2Topic.put(host + ":" + port + ":" + tableName, topic);
		params.add(new BasicString(this.localIP));
		params.add(new BasicInt(this.listeningPort));
		params.add(new BasicString(tableName));
		if (offset != -1)
		params.add(new BasicLong(offset));
		re = dbConn.run("publishTable", params);
		
		dbConn.close();
		return queue;
	}
	
	
	protected void unsubscribeInternal(String host,int port ,String tableName) throws IOException {
		
		Entity re;
		
		DBConnection dbConn = new DBConnection();
		
		dbConn.connect(host, port);
		
		List<Entity> params = new ArrayList<Entity>();
		params.add(new BasicString(this.localIP));
		params.add(new BasicInt(this.listeningPort));
		params.add(new BasicString(tableName));
		re = dbConn.run("stopPublishTable", params);
		dbConn.close();
		
		return;
		
	}
	
	
	public class SubTable{
		BasicStringVector f = null;
		String t = "";
		
		public SubTable(String topic,BasicStringVector fields){
			this.f = fields;
			this.t = topic;
		}
		
		public BasicStringVector getFields(){
			return this.f;
		}
		
		public String getTopic(){
			return this.t;
		}
	}
	public String GetLocalIP() throws SocketException{
	        try {
	            for (Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces(); interfaces.hasMoreElements();) {
	                NetworkInterface networkInterface = interfaces.nextElement();
	                if (networkInterface.isLoopback() || networkInterface.isVirtual() || !networkInterface.isUp()) {
	                    continue;
	                }
	                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
	                if (addresses.hasMoreElements()) {
	                    InetAddress ip = addresses.nextElement();
	                    return ip.getHostAddress();
	                }
	            }
	        } catch (SocketException e) {
	            throw e;
	        }
	        return null;
	}
}
