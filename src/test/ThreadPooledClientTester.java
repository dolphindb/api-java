package test;

import java.io.IOException;

import com.xxdb.streaming.client.ThreadPooledClient;

public class ThreadPooledClientTester {
	public static void main(String args[]) {
		ThreadPooledClient client = new ThreadPooledClient("192.168.1.13",8993,7);
		try {
			client.subscribe("192.168.1.42", 8801, "trades1", new TwoSigmaMessageHandler(), 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
