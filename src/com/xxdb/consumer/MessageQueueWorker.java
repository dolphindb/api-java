package com.xxdb.consumer;

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
	
	public MessageQueueWorker(ConsumerListenerManager lgnMgr){
		this._lsnMgr = lgnMgr;
	}
	// consume message in queue
	@Override
	public void run() {
		System.out.println("worker is fighting : find Message ");
		System.out.println(Consumer.getMessageQueue().size());

		while(true){
			if(Consumer.getMessageQueue().size()>0)
			{
				System.out.println("find messages in queue");
				IMessage msg = null;
				try {
					msg = Consumer.getMessageQueue().take();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				this._lsnMgr.fireEvent(msg);
				System.out.println("a message handled");
			}
		}
	}
	
}
