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
		System.out.println("worker is fighting : find Message ");

		SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
		int ct = 0;
		Date ds = new Date();
		Date dLastBusy = null;
		boolean isprinted = false;
		
		BlockingQueue<IMessage> queue = Daemon.getMessageQueue(this._topic);
		while(true){
			
			if(queue.isEmpty() == false)
			{
//				System.out.println("find messages in queue");
				IMessage msg = null;
				try {
					msg = queue.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				this._lsnMgr.fireEvent(msg);
				ct ++;
				
				dLastBusy = new Date();
				
//				System.out.println("consume one message");
//				System.out.println(df1.format(new Date()));
			}
			else {
				if(isprinted == false)
				{
					if(dLastBusy!=null){
						Date de = new Date();
						if(de.getTime() - dLastBusy.getTime() >=10000){
							TimingLogger.AddLog(ds.getTime(), dLastBusy.getTime(), ct);
							isprinted = true;
	//						int t = TimingLogger.getAvg();
	//						if(t>0) System.out.println("Average time: " + t);
						}
					}
				}
			}
				
		}
	}
	
}
