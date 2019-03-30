package com.xxdb.streaming.client;

import com.xxdb.streaming.client.IMessage;

import java.io.IOException;
import java.net.SocketException;
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
    public ThreadPooledClient() throws SocketException {
        this(DEFAULT_PORT, CORES);
    }

    public ThreadPooledClient(int subscribePort, int threadCount) throws SocketException{
        super(subscribePort);
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
    
    protected void doReconnect(Site site) {
		threadPool.shutdownNow();
    	while (true) {
			try {
				subscribe(site.host, site.port, site.tableName, site.actionName, site.handler, site.msgId + 1, true);
				System.out.println("Successfully reconnected and subscribed " + site.host + ":" + site.port + ":" + site.tableName);
				return;
			} catch (Exception ex) {
				System.out.println("Unable to subscribe table. Will try again after 5 seconds.");
				try {
					Thread.sleep(5000);
				} catch (Exception ex0){
					
				}
			}
		}
    }

    public void subscribe(String host,int port,String tableName,String actionName,MessageHandler handler,long offset,boolean reconnect) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port,tableName,actionName,offset,reconnect);
        synchronized (queueHandlers) {
            queueHandlers.put(tableNameToTopic.get(host + ":" + port + ":" + tableName), new QueueHandlerBinder(queue, handler));
        }
    }
    
    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset) throws IOException {
        subscribe(host, port, tableName, actionName, handler, offset, false);
    }
    
    public void subscribe(String host,int port,String tableName, String actionName, MessageHandler handler) throws IOException {
        subscribe(host, port, tableName, actionName, handler, -1);
    }
    
    public void subscribe(String host,int port,String tableName, String actionName, MessageHandler handler, boolean reconnect) throws IOException {
        subscribe(host, port, tableName, actionName, handler, -1, reconnect);
    }

    public void subscribe(String host,int port,String tableName,MessageHandler handler) throws IOException {
        subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, -1);
    }
    
    public void subscribe(String host,int port,String tableName,MessageHandler handler,boolean reconnect) throws IOException {
        subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, -1, reconnect);
    }
    
    public void subscribe(String host,int port,String tableName,MessageHandler handler,long offset) throws IOException {
        subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset);
    }
    
    public void subscribe(String host,int port,String tableName,MessageHandler handler,long offset,boolean reconnect) throws IOException {
        subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset, reconnect);
    }

    public void unsubscribe(String host,int port ,String tableName,String actionName) throws IOException {
        unsubscribeInternal(host, port, tableName,actionName);
    }

    public void unsubscribe(String host,int port ,String tableName) throws IOException {
        unsubscribeInternal(host, port, tableName);
    }
}
