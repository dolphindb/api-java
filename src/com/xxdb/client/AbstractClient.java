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
import com.xxdb.data.BasicString;
import com.xxdb.data.Entity;

public abstract class Client {
	private HandlerManager _lsnMgr = null;

	private int _workerNumber = 0;
	
	private int _listeningPort = 8849;

	private QueueManager _queueManager = new QueueManager();
	public Client(int subscribePort){
		this._lsnMgr = new HandlerManager();
		this._listeningPort = subscribePort;
		Daemon daemon = new Daemon(subscribePort, _queueManager);
		Thread pThread = new Thread(daemon);
		pThread.start();
	}

	public ArrayList<IMessage> poll(String topic, long timeout){

		ArrayList<IMessage> reArray = new ArrayList<IMessage>();
		BlockingQueue<IMessage> queue = _queueManager.getQueue(topic);
		Date ds = new Date();
		long start = ds.getTime();
        long remaining = timeout;
         
		do{
			if(queue.isEmpty() == false){
				try {
					reArray.add(queue.take());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			long elapsed = (new Date()).getTime() - start;
            remaining = timeout - elapsed;
		} while (remaining > 0);
		return reArray;
	}
	
	
	public void setConsumeThreadNumber (int number){
		this._workerNumber = number;
	}

	protected HandlerManager getConsumerListenerManager() {
		return this._lsnMgr;
	}

	private String handleSubscribe(String host,int port,String tableName,IncomingMessageHandler handler) {

		Entity re;
		DBConnection dbConn = new DBConnection();
		String topic = "";
		
		//start thread for socket accept when got a topic	
		try {
			dbConn.connect(host, port);

			List<Entity> params = new ArrayList<Entity>();

			params.add(new BasicString(tableName));
			re = dbConn.run("getSubscriptionTopic", params);
			topic = re.getString();
			System.out.println("getSubscriptionTopic:" + topic);
		
			if(handler!=null)
				this._lsnMgr.addIncomingMessageHandler(topic, handler);
			_queueManager.addQueue(topic);
			params.clear();

			params.add(new BasicString("localhost"));
			params.add(new BasicInt(this._listeningPort));
			params.add(new BasicString(tableName));
			re = dbConn.run("publishTable", params);
			System.out.println("publishTable:" + re.getString());

		} catch (IOException e) {
			e.printStackTrace();
		}	
		
		if(handler!=null){
			if(this._workerNumber<=1){
				MessageQueueWorker consumeQueueWorker = new MessageQueueWorker(topic,this._lsnMgr,_queueManager );
				Thread myThread2 = new Thread(consumeQueueWorker);
				myThread2.start();
			} else {
				ExecutorService pool = Executors.newCachedThreadPool();
				for (int i=0;i<this._workerNumber;i++) {
					pool.execute(new MessageQueueWorker(topic,this._lsnMgr, _queueManager));
				}
			}
		}
		return topic;
	}
	
	public void subscribe(String host,int port,String tableName,IncomingMessageHandler handler){
		handleSubscribe(host,port,tableName, handler);
	}
	
	public String subscribe(String host,int port,String tableName){
		return handleSubscribe(host,port,tableName,null);
	}
	
	
}
