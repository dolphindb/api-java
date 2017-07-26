package test;

import java.io.IOException;

import com.xxdb.client.ThreadPooledClient;

public class ThreadPooledClientTester {
	public static void main(String args[]) {
		ThreadPooledClient client = new ThreadPooledClient(8993,7);
		try {
			client.subscribe("192.168.1.13", 8849, "trades", new TwoSigmaMessageHandler());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
