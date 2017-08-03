package com.xxdb.streaming.sample;

import java.io.IOException;
import java.net.SocketException;

import com.xxdb.streaming.client.ThreadPooledClient;

public class ThreadPooledClientTester {
	public static void main(String args[]) {
		
		ThreadPooledClient client;
		try {
			client = new ThreadPooledClient(8993,7);
			client.subscribe("192.168.1.42", 8801, "trades1", new SampleMessageHandler(), 0);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
