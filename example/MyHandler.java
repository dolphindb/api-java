package com.dolphindb.examples;

import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;

public class MyHandler implements MessageHandler {
    public void doEvent(IMessage msg) {
        System.out.println(Thread.currentThread().getName() + " get a message: " + msg.getEntity(0).getString()
                + " " + msg.getEntity(1).getString()
                + " " + msg.getEntity(2).getString() + "..."
        );
    }
}