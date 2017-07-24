package com.xxdb.client;

import com.xxdb.client.datatransferobject.IMessage;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

/**
 * Created by root on 7/24/17.
 */
public class ThreadedClient extends  AbstractClient {
    // A ThreadedClient using the default subscribe port 8849.
    public ThreadedClient() {
        this(DEFAULT_PORT);
    }
    public ThreadedClient(int subscribePort){
        super(subscribePort);
    }
    class HandlerLopper extends Thread{
        BlockingQueue<IMessage> queue;
        IncomingMessageHandler handler;
        HandlerLopper(BlockingQueue<IMessage> queue, IncomingMessageHandler handler) {
            this.queue = queue;
            this.handler = handler;
        }
        public void run() {
            while(true) {
                try {
                    IMessage msg = queue.take();
                    handler.doEvent(msg);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void subscribe(String host,int port,String tableName, IncomingMessageHandler handler, long offset) throws IOException {
        BlockingQueue<IMessage> queue = subscribeTo(host,port,tableName, offset);
        new HandlerLopper(queue, handler).start();
    }
    public void subscribe(String host,int port,String tableName, IncomingMessageHandler handler) throws IOException {
        subscribe(host, port, tableName, handler, -1);
    }
}
