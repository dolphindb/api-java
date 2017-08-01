package com.xxdb.streaming.client;

import com.xxdb.streaming.client.IMessage;

import java.io.IOException;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.BlockingQueue;


public class ThreadedClient extends  AbstractClient {

	public ThreadedClient() throws SocketException {
        this(DEFAULT_PORT);
    }
    public ThreadedClient(int subscribePort) throws SocketException{
        super(subscribePort);
    }
    
    class HandlerLopper extends Thread{
        BlockingQueue<List<IMessage>> queue;
        MessageHandler handler;
        HandlerLopper(BlockingQueue<List<IMessage>> queue, MessageHandler handler) {
            this.queue = queue;
            this.handler = handler;
        }
        public void run() {
            while(true) {
                try {
                    List<IMessage> msgs = queue.take();
                    for (IMessage msg : msgs) {
                        handler.doEvent(msg);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void subscribe(String host, int port, String tableName, MessageHandler handler, long offset) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host,port,tableName, offset);
        new HandlerLopper(queue, handler).start();
    }
    public void subscribe(String host,int port,String tableName, MessageHandler handler) throws IOException {
        subscribe(host, port, tableName, handler, -1);
    }
    void unsubscribe(String host,int port ,String tableName) throws IOException {
        unsubscribeInternal(host, port, tableName);
    }
}
