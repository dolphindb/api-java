package com.xxdb.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.xxdb.DBConnection;
import com.xxdb.consumer.datatransferobject.IMessage;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicString;
import com.xxdb.data.Entity;

public class Consumer {
	
	private ConsumerListenerManager _LsnMgr = null;
	private String _host = "";
	private int _port = 0;
	// Global list , multithreading processing 
	//private static List<IMessage> _list = new ArrayList<IMessage>();
	private static BlockingQueue<IMessage> _list = new ArrayBlockingQueue<>(4096);
	
	public Consumer(String host,int port) {
		this._LsnMgr = new ConsumerListenerManager();
		this._host = host;
		this._port = port;
	}
	
	public static BlockingQueue<IMessage> getMessageQueue(){
		return _list;
	}
	
	
	public ConsumerListenerManager getConsumerListenerManager() {
		return this._LsnMgr;
	}
	
	public void subscribe(String tableName) {

		//start subscribe thread for listening incoming message
		CreateSubscribeListening subscribeClient = new CreateSubscribeListening();
		Thread myThread1 = new Thread(subscribeClient);
		myThread1.start();



		Entity re;
		DBConnection dbConn = new DBConnection();
		
		try {
			dbConn.connect(this._host, this._port);

			List<Entity> params = new ArrayList<Entity>();
			//1.取得生产者的topic
			params.add(new BasicString(tableName));
			re = dbConn.run("getSubscriptionTopic", params);
			System.out.println("getSubscriptionTopic:" + re.getString());
			
			params.clear();
			//2、通过publishTable把订阅者地址端口，订阅表名传过去
			params.add(new BasicString("localhost"));
			params.add(new BasicInt(60011));
			params.add(new BasicString(tableName));
			re = dbConn.run("publishTable", params);
			System.out.println("publishTable:" + re.getString());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		//start Worker thread to invoke consume handler 
		MessageQueueWorker consumeQueueWorker = new MessageQueueWorker(this._LsnMgr);
		Thread myThread2 = new Thread(consumeQueueWorker);
		myThread2.start();
	}
	
	public void addListener(MessageIncomingHandler listener){
		this._LsnMgr.addMessageIncomingListener(listener);
	}
}

