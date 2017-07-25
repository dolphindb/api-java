package test;

import com.xxdb.client.IncomingMessageHandler;
import com.xxdb.client.datatransferobject.IMessage;

public class TwoSigmaMessageHandler implements IncomingMessageHandler {
	private static long count = 0;
	private static long start = 0;
	private static long end = 0;
	private static boolean started = false;
	
	@Override
	public void doEvent(IMessage msg) {
		if(started==false){
			started = true;
			start = System.currentTimeMillis();
		}
		end = System.currentTimeMillis();
		count ++;
		 if((end - start) > 6000){
         	System.out.println(count + " messages took " + (end - start) + "ms, throughput: " + count / ((end - start) / 1000.0) + " messages/s");
         	
		 }
		 
	}

}
