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
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.IMessage;

abstract class AbstractClient implements MessageDispatcher{
	protected static final int DEFAULT_PORT = 8849;
	protected static final String DEFAULT_HOST = "localhost";
	protected static final String DEFAULT_ACTION_NAME = "javaStreamingApi";
	protected int listeningPort;
	protected QueueManager queueManager = new QueueManager();
	protected HashMap<String, List<IMessage>> messageCache = new HashMap<>();
	protected HashMap<String, String> tableNameToTopic = new HashMap<>();
	protected HashMap<String, Boolean> hostEndian = new HashMap<>();
	protected Thread pThread;
	protected HashMap<String, Site> topicToSite = new HashMap<>();
	
	protected class Site {
		String host;
		int port;
		String tableName;
		String actionName;
		MessageHandler handler;
		long msgId;
		boolean reconnect;
		Vector filter = null;
		boolean closed = false;

		Site(String host, int port, String tableName, String actionName,
				MessageHandler handler, long msgId, boolean reconnect, Vector filter) {
			this.host = host;
			this.port = port;
			this.tableName = tableName;
			this.actionName = actionName;
			this.handler = handler;
			this.msgId = msgId;
			this.reconnect = reconnect;
			this.filter = filter;
		}
	}
	
	abstract protected void doReconnect(Site site);
	
	public void setMsgId(String topic, long msgId) {
		synchronized (topicToSite) {
			Site site = topicToSite.get(topic);
			if (site != null)
				site.msgId = msgId;
		}
	}

	public void tryReconnect(String topic) {
		System.out.println("Trigger reconnect");
		queueManager.removeQueue(topic);
    	Site site = null;
    	synchronized (topicToSite) {
			site = topicToSite.get(topic);
		}
    	if (!site.reconnect)
    		return;
		activeCloseConnection(site);
		doReconnect(site);
	}
	
	public void activeCloseConnection(Site site) {
		while (true) {
			try {
				DBConnection conn = new DBConnection();
				conn.connect(site.host, site.port);
				try {
					String localIP = conn.getLocalAddress().getHostAddress();
					List<Entity> params = new ArrayList<>();
					params.add(new BasicString(localIP));
					params.add(new BasicInt(listeningPort));
					conn.run("activeClosePublishConnection", params);
				} catch (IOException ioex) {
					throw ioex;
				} finally {
					conn.close();
				}
				return;
			} catch (Exception ex) {
				System.out.println("Unable to actively close the publish connection from site " + site.host + ":" + site.port);
			}
			
			try {
				Thread.sleep(1000);
			} catch (Exception e) {

			}
		}
	}

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
	
	public boolean isClosed(String topic) {
		synchronized (topicToSite) {
			Site site = topicToSite.get(topic);
			if (site != null)
				return site.closed;
			else
				return true;
		}
	}
	
	protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port,
			String tableName, String actionName, MessageHandler handler,
			long offset, boolean reconnect, Vector filter)
			throws IOException,RuntimeException {
		Entity re;
		String topic = "";
		
		DBConnection dbConn = new DBConnection();
		dbConn.connect(host, port);
		try {
			String localIP = dbConn.getLocalAddress().getHostAddress();
			
			if(!hostEndian.containsKey(host)){
				hostEndian.put(host, dbConn.getRemoteLittleEndian());
			}
			
			List<Entity> params = new ArrayList<Entity>();
			params.add(new BasicString(tableName));
			params.add(new BasicString(actionName));
			re = dbConn.run("getSubscriptionTopic", params);
			topic = ((BasicAnyVector)re).getEntity(0).getString();
			params.clear();
			
			synchronized (tableNameToTopic) {
				tableNameToTopic.put(host + ":" + port + ":" + tableName, topic);
			}
			synchronized (topicToSite) {
				topicToSite.put(topic, new Site(host, port, tableName, actionName, handler, offset - 1, reconnect, filter));
			}

			params.add(new BasicString(localIP));
			params.add(new BasicInt(this.listeningPort));
			params.add(new BasicString(tableName));
			params.add(new BasicString(actionName));
			params.add(new BasicLong(offset));
			if (filter != null)
				params.add(filter);
			re = dbConn.run("publishTable", params);
		} catch (Exception ex) {
			throw ex;
		} finally {
			dbConn.close();
		}
		BlockingQueue<List<IMessage>> queue = queueManager.addQueue(topic);
		return queue;
	}
	
	protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port,
			String tableName, String actionName, long offset, boolean reconnect)
			throws IOException,RuntimeException {
		return subscribeInternal(host, port, tableName, actionName, null, offset, reconnect, null);
	}

	protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port, String tableName, long offset) throws IOException,RuntimeException {
		return subscribeInternal(host,port,tableName,DEFAULT_ACTION_NAME,offset,false);
	}
	
	protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port, String tableName, String actionName, long offset) throws IOException,RuntimeException {
		return subscribeInternal(host,port,tableName,actionName,offset,false);
	}
	
	protected void unsubscribeInternal(String host,int port ,String tableName,String actionName) throws IOException {
		DBConnection dbConn = new DBConnection();
		dbConn.connect(host, port);
		try {
			String localIP = dbConn.getLocalAddress().getHostAddress();
			List<Entity> params = new ArrayList<Entity>();
			params.add(new BasicString(localIP));
			params.add(new BasicInt(this.listeningPort));
			params.add(new BasicString(tableName));
			params.add(new BasicString(actionName));
			dbConn.run("stopPublishTable", params);
			String topic = null;
			String fullTableName = host + ":" + port + ":" + tableName;
			synchronized (tableNameToTopic) {
				topic = tableNameToTopic.get(fullTableName);
			}
			synchronized (topicToSite) {
				Site site = topicToSite.get(topic);
				if (site != null)
					site.closed = true;
			}
			System.out.println("Successfully unsubscribed table " + fullTableName);
		} catch (Exception ex) {
			throw ex;
		} finally {
			dbConn.close();
		}
		return;
	}

	protected void unsubscribeInternal(String host,int port ,String tableName) throws IOException {
		unsubscribeInternal(host, port, tableName, DEFAULT_ACTION_NAME);
	}
}
