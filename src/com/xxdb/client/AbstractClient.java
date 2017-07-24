package com.xxdb.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.xxdb.DBConnection;
import com.xxdb.client.datatransferobject.IMessage;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicLong;
import com.xxdb.data.BasicString;
import com.xxdb.data.Entity;

public abstract class AbstractClient {
	protected static final int DEFAULT_PORT = 8849;
	protected HandlerManager _lsnMgr = null;

	protected int _listeningPort;

	protected QueueManager _queueManager = new QueueManager();
	public AbstractClient(){
		this(DEFAULT_PORT);
	}
	public AbstractClient(int subscribePort){
		this._listeningPort = subscribePort;
		Daemon daemon = new Daemon(subscribePort, _queueManager);
		Thread pThread = new Thread(daemon);
		pThread.start();
	}

	// establish a connection between dolphindb server
	// return a queue exclusively for the table.
	protected BlockingQueue<IMessage> subscribeTo(String host,int port,String tableName, long offset) throws IOException,RuntimeException {

		Entity re;
		DBConnection dbConn = new DBConnection();
		String topic = "";

		//start thread for socket accept when got a topic
		dbConn.connect(host, port);

		List<Entity> params = new ArrayList<Entity>();

		params.add(new BasicString(tableName));
		re = dbConn.run("getSubscriptionTopic", params);
		topic = re.getString();
		System.out.println("getSubscriptionTopic:" + topic);
		BlockingQueue<IMessage> queue = _queueManager.addQueue(topic);
		params.clear();

		params.add(new BasicString("localhost"));
		params.add(new BasicInt(this._listeningPort));
		params.add(new BasicString(tableName));
		if (offset != -1)
		params.add(new BasicLong(offset));
		re = dbConn.run("publishTable", params);
		System.out.println("publishTable:" + re.getString());

		return queue;
	}
}
