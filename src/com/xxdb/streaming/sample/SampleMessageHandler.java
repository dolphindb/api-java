package com.xxdb.streaming.sample;

import com.xxdb.data.BasicInt;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import java.util.concurrent.atomic.AtomicLong;

public class SampleMessageHandler implements MessageHandler {
	private AtomicLong count = new AtomicLong();
	private long start = 0;

	private boolean started = false;
	private static int old = 0;
	@Override
	public void doEvent(IMessage msg) {
		if(started==false){
			started = true;
			start = System.currentTimeMillis();
		}

		count.incrementAndGet();

		if (count.get() % 100000 == 0) {
			long end = System.currentTimeMillis();
			System.out.println(count + " messages took " + (end - start) + "ms, throughput: " + count.get() / ((end - start) / 1000.0) + " messages/s");		 

		}
		if (count.get() == 20000000) {
			System.out.println("Done");
		}
//		
	}

}
