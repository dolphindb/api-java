package com.xxdb.client;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import com.xxdb.client.datatransferobject.IMessage;

public class MessageQueueWorker  implements Runnable{

	private ReentrantLock lock = new ReentrantLock(); 
	
	private HandlerManager _lsnMgr = null;
	
	private String _topic = "";

	private QueueManager _queueManager;
	public MessageQueueWorker(String topic,HandlerManager lgnMgr, QueueManager queueManager){
		this._lsnMgr = lgnMgr;
		this._topic = topic;
		this._queueManager = queueManager;
	}
	
	// consume message in queue
	@Override
	public void run() {
		BlockingQueue<IMessage> queue = _queueManager.getQueue(this._topic);
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
