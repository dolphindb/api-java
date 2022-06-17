package com.xxdb.streaming.client;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicString;
import com.xxdb.data.Entity;
import com.xxdb.data.Vector;

import java.io.IOException;
import java.net.SocketException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.time.LocalTime;
import java.util.concurrent.BlockingQueue;


public class ThreadedClient extends AbstractClient {
    private HashMap<String, HandlerLopper> handlerLoppers = new HashMap<>();

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
        private int batchSize = -1;
        private int throttle = -1;

        HandlerLopper(BlockingQueue<List<IMessage>> queue, MessageHandler handler) {
            this.queue = queue;
            this.handler = handler;
        }

        HandlerLopper(BlockingQueue<List<IMessage>> queue, MessageHandler handler, int batchSize, int throttle) {
            this.queue = queue;
            this.handler = handler;
            this.batchSize = batchSize;
            this.throttle = throttle;
        }

        public void run() {
            while (!isInterrupted()) {
                List<IMessage> msgs = null;
                if(batchSize == -1 && throttle == -1) {
                    try {
                        msgs = queue.take();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                else if(batchSize != -1 && throttle != -1){
                    LocalTime end = LocalTime.now().plusNanos(throttle*1000000);
                    while (msgs == null || ((msgs == null||msgs.size()<batchSize) && LocalTime.now().isBefore(end))){
                        List<IMessage> tmp = queue.poll();
                        if(tmp != null){
                            if(msgs == null)
                                msgs = tmp;
                            else
                                msgs.addAll(tmp);
                        }
                    }
                }
                else {
                    LocalTime end = LocalTime.now().plusNanos(throttle*1000000);
                    while (msgs == null || LocalTime.now().isBefore(end)){
                        List<IMessage> tmp = queue.poll();
                        if(tmp != null){
                            if(msgs == null)
                                msgs = tmp;
                            else
                                msgs.addAll(tmp);
                        }
                    }
                }
                if(msgs == null)
                    continue;
                for (IMessage msg : msgs) {
                    handler.doEvent(msg);
                }
            }
        }
    }

    @Override
    protected boolean doReconnect(Site site) {
        String topicStr = site.host + ":" + site.port + "/" + site.tableName + "/" + site.actionName;
        Thread handlerLopper = null;
        synchronized (handlerLoppers) {
            if (!handlerLoppers.containsKey(topicStr))
                throw new RuntimeException("Subscribe thread is not started");
            handlerLopper = handlerLoppers.get(topicStr);
        }
        handlerLopper.interrupt();
        try {
            subscribe(site.host, site.port, site.tableName, site.actionName, site.handler, site.msgId + 1, true, site.filter, site.deserializer, site.allowExistTopic, "", "");
            Date d = new Date();
            DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            System.out.println(df.format(d) + " Successfully reconnected and subscribed " + site.host + ":" + site.port + "/" + site.tableName + "/" + site.actionName);
            return true;
        } catch (Exception ex) {
            Date d = new Date();
            DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            System.out.println(df.format(d) + " Unable to subscribe table. Will try again after 1 seconds." + site.host + ":" + site.port + "/" + site.tableName + "/" + site.actionName);
            ex.printStackTrace();
            return false;
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, String userName, String password) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, password);
        HandlerLopper handlerLopper = new HandlerLopper(queue, handler);
        handlerLopper.start();
        String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
        synchronized (handlerLoppers) {
            handlerLoppers.put(topicStr, handlerLopper);
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, int batchSize, int throttle, String userName, String password) throws IOException {
        if(batchSize<=0)
            throw new IllegalArgumentException("BatchSize must be greater than zero");
        if(throttle<0)
            throw new IllegalArgumentException("Throttle must be greater than or equal to zero");
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, password);
        HandlerLopper handlerLopper = new HandlerLopper(queue, handler, batchSize, throttle == 0 ? -1 : throttle);
        handlerLopper.start();
        String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
        synchronized (handlerLoppers) {
            handlerLoppers.put(topicStr, handlerLopper);
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, int batchSize, int throttle) throws IOException{
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, batchSize, throttle, "", "");
    }


    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic)throws IOException{
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter,  deserializer, allowExistTopic, "", "");
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic) throws IOException{
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter,  null, allowExistTopic);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer)throws Exception{
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter,  deserializer, true, "", "");
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic, int batchSize, int throttle)throws IOException{
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, null, allowExistTopic, batchSize, throttle);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic, int batchSize, int throttle, String userName, String passWord)throws IOException{
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, null, allowExistTopic, batchSize, throttle, userName, passWord);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, String userName, String password) throws IOException{
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, null, null, false, userName, password);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect) throws IOException {
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, null, null, false, "", "");
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, Vector filter) throws IOException {
        subscribe(host, port, tableName, actionName, handler, offset, false, filter, null, false, "", "");
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

    public void subscribe(String host, int port, String tableName, MessageHandler handler, long offset, boolean reconnect, String userName, String password) throws IOException{
        subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset, reconnect, userName, password);
    }

    public void unsubscribe(String host, int port, String tableName, String actionName) throws IOException {
        unsubscribeInternal(host, port, tableName, actionName);
    }

    public void unsubscribe(String host, int port, String tableName) throws IOException {
        unsubscribeInternal(host, port, tableName);
    }

    @Override
    protected void unsubscribeInternal(String host, int port, String tableName, String actionName) throws IOException {
        DBConnection dbConn = new DBConnection();
        dbConn.connect(host, port);
        try {
            String localIP = this.listeningHost;
            if(localIP.equals(""))
                localIP = dbConn.getLocalAddress().getHostAddress();
            List<Entity> params = new ArrayList<Entity>();
            params.add(new BasicString(localIP));
            params.add(new BasicInt(this.listeningPort));
            params.add(new BasicString(tableName));
            params.add(new BasicString(actionName));

            dbConn.run("stopPublishTable", params);
            String topic = null;
            String fullTableName = host + ":" + port + "/" + tableName + "/" + actionName;
            synchronized (tableNameToTrueTopic) {
                topic = tableNameToTrueTopic.get(fullTableName);
            }
            synchronized (trueTopicToSites) {
                Site[] sites = trueTopicToSites.get(topic);
                if (sites == null || sites.length == 0)
                    ;
                for (int i = 0; i < sites.length; i++)
                    sites[i].closed = true;
            }
           synchronized (queueManager) {
               queueManager.removeQueue(topic);
           }
            System.out.println("Successfully unsubscribed table " + fullTableName);
        } catch (Exception ex) {
            throw ex;
        } finally {
            dbConn.close();
            String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
            HandlerLopper handlerLopper = null;
            synchronized (handlerLoppers) {
                handlerLopper = handlerLoppers.get(topicStr);
                handlerLoppers.remove(topicStr);
                handlerLopper.interrupt();
            }
        }
        return;
    }
    public void close(){
        synchronized (handlerLoppers) {
            Iterator<HandlerLopper> it = handlerLoppers.values().iterator();
            while (it.hasNext()) {
                it.next().interrupt();
            }
            handlerLoppers.clear();
        }
        pThread.interrupt();
    }
}
