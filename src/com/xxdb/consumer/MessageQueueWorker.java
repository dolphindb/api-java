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
	
	public MessageQueueWorker(ConsumerListenerManager lgnMgr){
		this._lsnMgr = lgnMgr;
	}
	// consume message in queue
	@Override
	public void run() {
		System.out.println("worker is fighting : find Message ");
//		System.out.println(Consumer.getMessageQueue().size());
		SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
		int ct = 0;
		Date ds = new Date();
		Date dLastBusy = null;
		boolean isprinted = false;
		while(true){
			
			if(Consumer.getMessageQueue().size()>0)
			{
//				System.out.println("find messages in queue");
				IMessage msg = null;
				try {
					msg = Consumer.getMessageQueue().take();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
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
					//计算统计均值
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
