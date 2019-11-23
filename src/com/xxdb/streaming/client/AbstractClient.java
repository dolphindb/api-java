package com.xxdb.streaming.client;

import java.io.IOException;
import java.net.SocketException;
import java.util.*;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.lang.Iterable;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.data.Void;
import com.xxdb.streaming.client.IMessage;

abstract class AbstractClient implements MessageDispatcher{
	protected static final int DEFAULT_PORT = 8849;
	protected static final String DEFAULT_HOST = "localhost";
	protected static final String DEFAULT_ACTION_NAME = "javaStreamingApi";

	protected ConcurrentHashMap<String, AtomicBoolean> reconnectTable = new ConcurrentHashMap<String, AtomicBoolean>();

	protected int listeningPort;
	protected QueueManager queueManager = new QueueManager();
	protected HashMap<String, List<IMessage>> messageCache = new HashMap<>();
	protected HashMap<String, String> tableNameToTrueTopic = new HashMap<>();
	protected HashMap<String, String> HATopicToTrueTopic = new HashMap<>();
	protected HashMap<String, Boolean> hostEndian = new HashMap<>();
	protected Thread pThread;
	protected HashMap<String, Site[]> trueTopicToSites = new HashMap<>();

	public void setNeedReconnect(String topic ,boolean v){
			reconnectTable.put(topic, new AtomicBoolean(v));
			System.out.println("set reconnect signal " + String.valueOf(v));
	}

	public boolean getNeedReconnect(String topic){
		AtomicBoolean isRec = this.reconnectTable.get(topic);
		if(isRec!=null)
			return isRec.get();
		else
			return false;
	}

	public List<String> getAllTopics(){
		return queueManager.getAllTopic();
	}

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
		boolean allowExistTopic = false;
		Site(String host, int port, String tableName, String actionName,
				MessageHandler handler, long msgId, boolean reconnect, Vector filter, boolean allowExistTopic) {
			this.host = host;
			this.port = port;
			this.tableName = tableName;
			this.actionName = actionName;
			this.handler = handler;
			this.msgId = msgId;
			this.reconnect = reconnect;
			this.filter = filter;
			this.allowExistTopic = allowExistTopic;
		}
	}
	
	abstract protected void doReconnect(Site site);
	
	public void setMsgId(String topic, long msgId) {
		synchronized (trueTopicToSites) {
			Site[] sites = trueTopicToSites.get(topic);
			if (sites == null || sites.length == 0)
				return;
			if (sites.length == 1)
				sites[0].msgId = msgId;
		}
	}

	public void tryReconnect(String topic) {
		System.out.println("Trigger reconnect");
		AtomicBoolean recSignal = reconnectTable.get(topic);
		if(recSignal==null) return;
		synchronized (recSignal) {
			topic = HATopicToTrueTopic.get(topic);
			queueManager.removeQueue(topic);
			Site[] sites = null;
			synchronized (trueTopicToSites) {
				sites = trueTopicToSites.get(topic);
			}
			if (sites == null || sites.length == 0)
				return;
			if (sites.length == 1) {
				if (!sites[0].reconnect)
					return;
			}
			Site site = activeCloseConnection(sites);
			doReconnect(site);
		}
	}
	
	private Site activeCloseConnection(Site[] sites) {
		int siteId = 0;
		int siteNum = sites.length;
		while (true) {
			Site site = sites[siteId];
			siteId = (siteId + 1) % siteNum;
			try {
				DBConnection conn = new DBConnection();
				conn.connect(site.host, site.port);
				try {
					String localIP = conn.getLocalAddress().getHostAddress();
					List<Entity> params = new ArrayList<>();
					params.add(new BasicString(localIP));
					params.add(new BasicInt(listeningPort));
					conn.run("activeClosePublishConnection", params);
					System.out.println("Successfully closed publish connection");
					return site;
				} catch (IOException ioex) {
					throw ioex;
				} finally {
					conn.close();
				}
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
		String topicString = msg.getTopic();
		String[] topics = topicString.split(",");
		for (String topic:topics) {
			topic = HATopicToTrueTopic.get(topic);
			List<IMessage> cache = messageCache.get(topic);
			if (cache == null) {
				cache = new ArrayList<>();
				messageCache.put(topic, cache);
			}
			cache.add(msg);
		}

	}
	
	private synchronized void flushToQueue() {
		Set<String> keySet = messageCache.keySet();
		for(String topic : keySet) {
			try {
				BlockingQueue<List<IMessage>> q = queueManager.getQueue(topic);
				if(q!=null)
					q.put(messageCache.get(topic));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		messageCache.clear();
	}

	public synchronized void dispatch(IMessage msg) {
		String topicString = msg.getTopic();
		String[] topics = topicString.split(",");
		for (String topic:topics) {
			topic = HATopicToTrueTopic.get(topic);
			BlockingQueue<List<IMessage>> queue = queueManager.getQueue(topic);
			try {
				if(queue!=null)
					queue.put(Arrays.asList(msg));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
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
	
	public synchronized boolean isClosed(String topic) {
		topic = HATopicToTrueTopic.get(topic);
		synchronized (trueTopicToSites) {
			Site[] sites = trueTopicToSites.get(topic);
			if (sites == null || sites.length == 0)
				return true;
			else
				return sites[0].closed;
		}
	}
	
	private String getTopic(String host, int port, String alias, String tableName, String actionName) {
		return String.format("%s:%d:%s/%s/%s", host, port, alias, tableName, actionName);
	}

	protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port,
			String tableName, String actionName, MessageHandler handler,
			long offset, boolean reconnect, Vector filter, boolean allowExistTopic)
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

			params.add(new BasicString(localIP));
			params.add(new BasicInt(this.listeningPort));
			params.add(new BasicString(tableName));
			params.add(new BasicString(actionName));
			params.add(new BasicLong(offset));
			if (filter != null)
				params.add(filter);
			else{
				params.add(new Void());
			}
			params.add(new BasicBoolean(allowExistTopic));

			re = dbConn.run("publishTable", params);

			if (re instanceof BasicAnyVector) {
				BasicStringVector HASiteStrings = (BasicStringVector)(((BasicAnyVector) re).getEntity(1));
				int HASiteNum = HASiteStrings.rows();
				Site[] sites = new Site[HASiteNum];
				for (int i = 0; i < HASiteNum; i++) {
					String HASite = HASiteStrings.getString(i);
					String[] HASiteHostAndPort = HASite.split(":");
					String HASiteHost = HASiteHostAndPort[0];
					int HASitePort = new Integer(HASiteHostAndPort[1]);
					String HASiteAlias = HASiteHostAndPort[2];
					sites[i] = new Site(HASiteHost, HASitePort, tableName, actionName, handler, offset - 1, true, filter, allowExistTopic);
					synchronized (tableNameToTrueTopic) {
						tableNameToTrueTopic.put(HASiteHost + ":" + HASitePort + ":" + tableName, topic);
					}
					String HATopic = getTopic(HASiteHost, HASitePort, HASiteAlias, tableName, actionName);
					synchronized (HATopicToTrueTopic) {
						HATopicToTrueTopic.put(HATopic, topic);
					}
				}
				synchronized (trueTopicToSites) {
					trueTopicToSites.put(topic, sites);
				}
			}
			else {
				Site[] sites = {new Site(host, port, tableName, actionName, handler, offset - 1, reconnect, filter,allowExistTopic)};
				synchronized (tableNameToTrueTopic) {
					tableNameToTrueTopic.put(host + ":" + port + ":" + tableName, topic);
				}
				synchronized (HATopicToTrueTopic) {
					HATopicToTrueTopic.put(topic, topic);
				}
				synchronized (trueTopicToSites) {
					trueTopicToSites.put(topic, sites);
				}
			}
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
		return subscribeInternal(host, port, tableName, actionName, null, offset, reconnect, null,false);
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
			synchronized (tableNameToTrueTopic) {
				topic = tableNameToTrueTopic.get(fullTableName);
			}
			synchronized (trueTopicToSites) {
				Site[] sites = trueTopicToSites.get(topic);
				if (sites == null || sites.length == 0)
					;
				for (int i = 0; i < sites.length; i++)
					sites[i].closed = true;
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
