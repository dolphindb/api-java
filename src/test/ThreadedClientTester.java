package test;

import java.io.IOException;

import com.xxdb.client.PollingClient;
import com.xxdb.client.ThreadedClient;

public class ThreadedClientTester {
	public static void main(String args[]) {
		
        ThreadedClient client = new ThreadedClient(8991);
        
        try {
			client.subscribe("192.168.1.12", 8082, "trades", new TwoSigmaMessageHandler());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
