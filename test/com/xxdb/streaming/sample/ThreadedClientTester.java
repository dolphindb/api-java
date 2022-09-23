package com.xxdb.streaming.sample;

import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import com.xxdb.streaming.client.ThreadedClient;

import java.io.IOException;
import java.net.SocketException;

public class ThreadedClientTester {

    public static void main(String args[]) throws SocketException {
        int port = Integer.parseInt(args[0]);
        String act = args[1];
        //String tb = args[2];
        ThreadedClient client = new ThreadedClient(port);
        try {
            client.subscribe("192.168.0.21", 9002, "st", act, new SampleMessageHandler());
            client.subscribe("192.168.0.21", 9002, "st2", act, new SampleMessageHandler());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
