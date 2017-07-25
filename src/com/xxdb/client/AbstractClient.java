package com.xxdb.client;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import com.xxdb.DBConnection;
import com.xxdb.client.datatransferobject.IMessage;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicLong;
import com.xxdb.data.BasicString;
import com.xxdb.data.Entity;

public abstract class AbstractClient {
	
	protected static final int DEFAULT_PORT = 8849;
	protected HandlerManager _lsnMgr = null;

	protected int listeningPort;

	protected QueueManager queueManager = new QueueManager();
	public AbstractClient(){
		this(DEFAULT_PORT);
	}
	public AbstractClient(int subscribePort){
		this.listeningPort = subscribePort;
		Daemon daemon = new Daemon(subscribePort, queueManager);
		Thread pThread = new Thread(daemon);
		pThread.start();
	}

	// establish a connection between dolphindb server
	// return a queue exclusively for the table.
	protected BlockingQueue<IMessage> subscribeTo(String host,int port,String tableName, long offset) throws IOException,RuntimeException {

		Entity re;
		DBConnection dbConn = new DBConnection();
		String topic = "";

		dbConn.connect(host, port);

		List<Entity> params = new ArrayList<Entity>();

		params.add(new BasicString(tableName));
		re = dbConn.run("getSubscriptionTopic", params);
		topic = re.getString();
		BlockingQueue<IMessage> queue = queueManager.addQueue(topic);
		params.clear();

		params.add(new BasicString(GetLocalIP()));
		params.add(new BasicInt(this.listeningPort));
		params.add(new BasicString(tableName));
		if (offset != -1)
		params.add(new BasicLong(offset));
		re = dbConn.run("publishTable", params);

		return queue;
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
