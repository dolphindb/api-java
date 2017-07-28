package com.xxdb.streaming.client;

import com.xxdb.streaming.client.datatransferobject.IMessage;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;


public class ThreadPooledClient extends AbstractClient {

	private static int CORES = Runtime.getRuntime().availableProcessors();
    private ExecutorService threadPool;
    private class QueueHandlerBinder {
        public QueueHandlerBinder(BlockingQueue<List<IMessage>> queue, MessageHandler handler) {
            this.queue = queue;
            this.handler = handler;
        }

        private BlockingQueue<List<IMessage>> queue;
        private MessageHandler handler;
    }
    private HashMap<String, QueueHandlerBinder> queueHandlers = new HashMap<>();
    public ThreadPooledClient() {
        this(DEFAULT_HOST,DEFAULT_PORT, CORES);
    }
    public ThreadPooledClient(String localIP,int subscribePort, int threadCount){
        super(localIP,subscribePort);
        threadPool = Executors.newFixedThreadPool(threadCount);
        new Thread() {
            private LinkedList<IMessage> backlog = new LinkedList<>();

            private boolean fillBacklog() {
                boolean filled = true;
                synchronized (queueHandlers) {
                    Set<String> keySet = queueHandlers.keySet();
                    for (String topic : keySet) {
                        List<IMessage> messages = queueHandlers.get(topic).queue.poll();
                        if (messages != null) {
                            backlog.addAll(messages);
                            filled = true;
                        }
                    }
                }
                return filled;
            }

            private void refill() {
                int count = 200;
                while(fillBacklog() == false) {
                    if (count > 100) {
                        ;
                    } else if (count > 0) {
                        Thread.yield();
                    } else {
                        LockSupport.park();
                    }
                    count = count - 1;
                }
            }

            public void run() {
                while (true) {
                    IMessage msg;
                    while ((msg = backlog.poll()) != null) {
                        QueueHandlerBinder binder;
                        synchronized (queueHandlers) {
                            binder = queueHandlers.get(msg.getTopic());
                        }
                        threadPool.execute(new HandlerRunner(binder.handler, msg));
                    }
                    refill();
                }
            }
        }.start();
    }
    class HandlerRunner implements Runnable{
        MessageHandler handler;
        IMessage message;
        HandlerRunner(MessageHandler handler, IMessage message) {
            this.handler = handler;
            this.message = message;
        }
        public void run() {
            this.handler.doEvent(message);
        }
    }
    public void subscribe(String host, int port, String tableName, MessageHandler handler, long offset) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port,tableName, offset);
        synchronized (queueHandlers) {
            queueHandlers.put(tableName2Topic.get(host + ":" + port + ":" + tableName), new QueueHandlerBinder(queue, handler));
        }
    }
    // subscribe to host:port on tableName with offset set to position past the last element
    public void subscribe(String host,int port,String tableName, MessageHandler handler) throws IOException {
        subscribe(host, port, tableName, handler, -1);
    }
    void unsubscribe(String host,int port ,String tableName) throws IOException {
        unsubscribeInternal(host, port, tableName);
    }
}
