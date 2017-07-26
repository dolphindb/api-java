package test;

import java.io.IOException;

import com.xxdb.streaming.client.ThreadPooledClient;

public class ThreadPooledClientTester {
	public static void main(String args[]) {
		ThreadPooledClient client = new ThreadPooledClient(8993,7);
		try {
			client.subscribe("192.168.1.25", 8848, "trades", new TwoSigmaMessageHandler(), 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
