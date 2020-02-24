package com.xxdb.streaming.sample;

import com.xxdb.streaming.client.ThreadedClient;

import java.io.IOException;
import java.net.SocketException;

public class ThreadedClientTester {


    public static void stopPublish() {

    }

    public static void main(String args[]) throws SocketException {

        ThreadedClient client = new ThreadedClient(8997);
        try {
            client.subscribe("192.168.1.42", 8904, "trades1", "", new SampleMessageHandler());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
