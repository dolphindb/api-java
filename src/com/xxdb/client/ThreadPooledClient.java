package com.xxdb.client;

import com.xxdb.client.datatransferobject.IMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by root on 7/24/17.
 */
public class ThreadPooledClient extends  AbstractClient {
    // A ThreadedClient using the default subscribe port 8849 and thread count.
    private static int CORES = Runtime.getRuntime().availableProcessors();
    private ExecutorService threadPool;
    private ArrayList<BlockingQueue<IMessage>> topicQueues = new ArrayList<>();
    private ArrayList<IncomingMessageHandler> handlers = new ArrayList<>();
    public ThreadPooledClient() {
        this(DEFAULT_PORT, CORES);
    }
    public ThreadPooledClient(int subscribePort, int threadCount){
        super(subscribePort);
        threadPool = Executors.newFixedThreadPool(threadCount);
        new Thread() {
            public void run() {
                while (true) {
                    boolean scheduled = false;
                    synchronized (topicQueues) {
                        for (int i = 0; i < topicQueues.size(); ++i) {
                            IMessage msg = topicQueues.get(i).poll();
                            if (msg != null) {
                                threadPool.execute(new HandlerRunner(handlers.get(i), msg));
                                scheduled = true;
                            }
                        }
                    }
                    if (scheduled == false) {
                        try {
                            synchronized (_queueManager) {
                                _queueManager.wait();
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }.start();
    }
    class HandlerRunner implements Runnable{
        IncomingMessageHandler handler;
        IMessage message;
        HandlerRunner(IncomingMessageHandler handler, IMessage message) {
            this.handler = handler;
        }
        public void run() {
            this.handler.doEvent(message);
        }
    }
    public void subscribe(String host,int port,String tableName, IncomingMessageHandler handler, long offset) throws IOException {
        BlockingQueue<IMessage> queue = subscribeTo(host, port,tableName, offset);
        synchronized (topicQueues) {
            topicQueues.add(queue);
            handlers.add(handler);
        }
    }
    // subscribe to host:port on tableName with offset set to position past the last element
    public void subscribe(String host,int port,String tableName, IncomingMessageHandler handler) throws IOException {
        subscribe(host, port, tableName, handler, -1);
    }
}
