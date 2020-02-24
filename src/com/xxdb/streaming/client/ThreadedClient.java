package com.xxdb.streaming.client;

import com.xxdb.data.Vector;
import com.xxdb.streaming.client.IMessage;

import java.io.IOException;
import java.net.SocketException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;


public class ThreadedClient extends AbstractClient {
    private HandlerLopper handlerLopper = null;

    public ThreadedClient() throws SocketException {
        this(DEFAULT_PORT);
    }

    public ThreadedClient(int subscribePort) throws SocketException {
        super(subscribePort);
    }

    public ThreadedClient(String subscribeHost, int subscribePort) throws SocketException {
        super(subscribeHost, subscribePort);
    }
    class HandlerLopper extends Thread {
        BlockingQueue<List<IMessage>> queue;
        MessageHandler handler;

        HandlerLopper(BlockingQueue<List<IMessage>> queue, MessageHandler handler) {
            this.queue = queue;
            this.handler = handler;
        }

        public void run() {
            while (true) {
                try {
                    List<IMessage> msgs = queue.take();
                    for (IMessage msg : msgs) {
                        handler.doEvent(msg);
                    }
                } catch (InterruptedException e) {
                    System.out.println("Handler thread stopped.");
                }
            }
        }
    }

    @Override
    protected boolean doReconnect(Site site) {
        if (handlerLopper == null)
            throw new RuntimeException("Subscribe thread is not started");
        handlerLopper.interrupt();
        try {
            subscribe(site.host, site.port, site.tableName, site.actionName, site.handler, site.msgId + 1, true, site.filter, site.allowExistTopic);
            Date d = new Date();
            DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//设置显示格式
            System.out.println(df.format(d) + " Successfully reconnected and subscribed " + site.host + ":" + site.port + ":" + site.tableName);
            return true;
        } catch (Exception ex) {
            Date d = new Date();
            DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//设置显示格式
            System.out.println(df.format(d) + " Unable to subscribe table. Will try again after 1 seconds." + site.host + ":" + site.port + ":" + site.tableName);
            ex.printStackTrace();
            return false;
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, allowExistTopic);
        handlerLopper = new HandlerLopper(queue, handler);
        handlerLopper.start();
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect) throws IOException {
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, null, false);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, Vector filter) throws IOException {
        subscribe(host, port, tableName, actionName, handler, offset, false, filter, false);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset) throws IOException {
        subscribe(host, port, tableName, actionName, handler, offset, false);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler) throws IOException {
        subscribe(host, port, tableName, actionName, handler, -1);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, boolean reconnect) throws IOException {
        subscribe(host, port, tableName, actionName, handler, -1, reconnect);
    }

    public void subscribe(String host, int port, String tableName, MessageHandler handler) throws IOException {
        subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, -1);
    }

    public void subscribe(String host, int port, String tableName, MessageHandler handler, boolean reconnect) throws IOException {
        subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, -1, reconnect);
    }

    public void subscribe(String host, int port, String tableName, MessageHandler handler, long offset) throws IOException {
        subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset);
    }

    public void subscribe(String host, int port, String tableName, MessageHandler handler, long offset, boolean reconnect) throws IOException {
        subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset, reconnect);
    }

    public void unsubscribe(String host, int port, String tableName, String actionName) throws IOException {
        unsubscribeInternal(host, port, tableName, actionName);
    }

    public void unsubscribe(String host, int port, String tableName) throws IOException {
        unsubscribeInternal(host, port, tableName);
    }
}
