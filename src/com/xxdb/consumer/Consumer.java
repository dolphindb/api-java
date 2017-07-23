package com.xxdb.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.xxdb.DBConnection;
import com.xxdb.consumer.datatransferobject.IMessage;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicString;
import com.xxdb.data.Entity;

public class Consumer {
	
	private ConsumerListenerManager _lsnMgr = null;
	private String _host = "";
	private int _port = 0;

	private static BlockingQueue<IMessage> _list = new ArrayBlockingQueue<>(4096);
	
	private int _workerNumber = 0;
	
	public Consumer(String host,int port) {
		this._lsnMgr = new ConsumerListenerManager();
		this._host = host;
		this._port = port;
	}
	
	public void setConsumeThreadNumber (int number){
		this._workerNumber = number;
	}
	
	public static BlockingQueue<IMessage> getMessageQueue(){
		return _list;
	}
	
	public ConsumerListenerManager getConsumerListenerManager() {
		return this._lsnMgr;
	}
	
	public void subscribe(String tableName,MessageIncomingHandler handler) {

		CreateSubscribeListening subscribeClient = new CreateSubscribeListening();
		Thread myThread1 = new Thread(subscribeClient);
		myThread1.start();



		Entity re;
		DBConnection dbConn = new DBConnection();
		
		try {
			dbConn.connect(this._host, this._port);

			List<Entity> params = new ArrayList<Entity>();

			params.add(new BasicString(tableName));
			re = dbConn.run("getSubscriptionTopic", params);
			String topic = re.getString();
			System.out.println("getSubscriptionTopic:" + topic);

			this._lsnMgr.addMessageIncomingListener(topic, handler);
			
			params.clear();

			params.add(new BasicString("localhost"));
			params.add(new BasicInt(60011));
			params.add(new BasicString(tableName));
			re = dbConn.run("publishTable", params);
			System.out.println("publishTable:" + re.getString());

		} catch (IOException e) {

			e.printStackTrace();
		}	
		
		
		if(this._workerNumber<=1){
			MessageQueueWorker consumeQueueWorker = new MessageQueueWorker(this._lsnMgr);
			Thread myThread2 = new Thread(consumeQueueWorker);
			myThread2.start();
		} else {
			ExecutorService pool = Executors.newCachedThreadPool();
			for (int i=0;i<this._workerNumber;i++) {
				pool.execute(new MessageQueueWorker(this._lsnMgr));
			}
		}
		
	}
	
}

