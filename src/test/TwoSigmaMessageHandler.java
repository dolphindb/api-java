package test;

import com.xxdb.client.IncomingMessageHandler;
import com.xxdb.client.datatransferobject.IMessage;
import com.xxdb.data.BasicInt;

public class TwoSigmaMessageHandler implements IncomingMessageHandler {
	private static long count = 0;
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
		long end = System.currentTimeMillis();
		count ++;
//		
//		int cur = ((int)(end - start) / 1000);
//		
//		if(cur > old){
//			old = cur;
//			System.out.println(count + " messages took " + (end - start) + "ms, throughput: " + count / ((end - start) / 1000.0) + " messages/s");
//		}
		 
//		BasicInt qty = msg.getValue(2);
//		if(qty.getInt() == -1){
			System.out.println(count + " messages took " + (end - start) + "ms, throughput: " + count / ((end - start) / 1000.0) + " messages/s");
//		}
	}

}
