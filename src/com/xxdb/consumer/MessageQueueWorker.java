package com.xxdb.consumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.xxdb.consumer.datatransferobject.IMessage;

public class MessageQueueWorker  implements Runnable{

	private ReentrantLock lock = new ReentrantLock(); 
	
	private ConsumerListenerManager _lsnMgr = null;
	
	private String _topic = "";
	
	public MessageQueueWorker(String topic,ConsumerListenerManager lgnMgr){
		this._lsnMgr = lgnMgr;
		this._topic = topic;
	}
	
	// consume message in queue
	@Override
	public void run() {
		BlockingQueue<IMessage> queue = Daemon.getMessageQueue(this._topic);
		while(true){
			
			if(queue.isEmpty() == false)
			{
				IMessage msg = null;
				try {
					msg = queue.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				this._lsnMgr.fireEvent(msg);
			}
				
		}
	}
	
}
