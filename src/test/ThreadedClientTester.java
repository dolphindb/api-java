package test;

import java.io.IOException;

import com.xxdb.client.PollingClient;
import com.xxdb.client.ThreadedClient;

public class ThreadedClientTester {
	public static void main(String args[]) {
        ThreadedClient client = new ThreadedClient(8990);
        
        try {
			client.subscribe("192.168.1.25", 8801, "trades1", new TwoSigmaMessageHandler());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
