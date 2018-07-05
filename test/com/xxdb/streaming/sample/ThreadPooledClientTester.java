package com.xxdb.streaming.sample;

import com.xxdb.streaming.client.ThreadPooledClient;

import java.io.IOException;
import java.net.SocketException;

public class ThreadPooledClientTester {
	public static void main(String args[]) {
		
		ThreadPooledClient client;
		try {
			client = new ThreadPooledClient(8993,7);
			client.subscribe("192.168.1.42", 8801, "trades1", new SampleMessageHandler(), 0);
		} catch (SocketException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
