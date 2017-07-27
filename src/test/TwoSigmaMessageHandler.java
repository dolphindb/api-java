package test;

import com.xxdb.data.BasicInt;
import com.xxdb.streaming.client.MessageHandler;
import com.xxdb.streaming.client.datatransferobject.IMessage;

import java.util.concurrent.atomic.AtomicLong;

public class TwoSigmaMessageHandler implements MessageHandler {
	private static AtomicLong count = new AtomicLong();
	private static long start = 0;
	//private static long end = 0;
	private static boolean started = false;
	private static int old = 0;
	@Override
	public void doEvent(IMessage msg) {
		if(started==false){
			started = true;
			start = System.currentTimeMillis();
		}

		count.incrementAndGet();
//		
//		int cur = ((int)(end - start) / 1000);
//		
//		if(cur > old){
//			old = cur;
//			System.out.println(count + " messages took " + (end - start) + "ms, throughput: " + count / ((end - start) / 1000.0) + " messages/s");
//		}
		 
		BasicInt qty = msg.getValue(2);
		if(qty.getInt() == -1){
			long end = System.currentTimeMillis();
			System.out.println(count + " messages took " + (end - start) + "ms, throughput: " + count.get() / ((end - start) / 1000.0) + " messages/s");
			System.exit(0);
		}
	}

}
