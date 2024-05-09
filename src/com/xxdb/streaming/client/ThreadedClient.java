package com.xxdb.streaming.client;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicString;
import com.xxdb.data.Entity;
import com.xxdb.data.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


public class ThreadedClient extends AbstractClient {
    private HashMap<String, HandlerLopper> handlerLoppers = new HashMap<>();
    // private HashMap<String, List<String>> users = new HashMap<>();

    private static final Logger log = LoggerFactory.getLogger(ThreadedClient.class);

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
        BatchMessageHandler batchHandler;
        private int batchSize = -1;
        private int throttle = -1;
        private float secondThrottle = -1.0f;

        HandlerLopper(BlockingQueue<List<IMessage>> queue, BatchMessageHandler batchMessageHandler, int batchSize, float secondThrottle){
            this.queue = queue;
            this.batchHandler = batchMessageHandler;
            this.batchSize = batchSize;
            this.secondThrottle = secondThrottle;
        }

        HandlerLopper(BlockingQueue<List<IMessage>> queue, MessageHandler handler, int batchSize, float secondThrottle) {
            this.queue = queue;
            this.handler = handler;
            this.batchSize = batchSize;
            this.secondThrottle = secondThrottle;
        }

        HandlerLopper(BlockingQueue<List<IMessage>> queue, BatchMessageHandler batchHandler, int batchSize, int throttle) {
            this.queue = queue;
            this.batchHandler = batchHandler;
            this.batchSize = batchSize;
            this.throttle = throttle;
        }

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
            while (!isClose()) {
                List<IMessage> msgs = null;
                if(batchSize == -1 && throttle == -1 || batchSize == -1 && secondThrottle == -1.0f) {
                    try {
                        msgs = queue.take();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                else if(batchSize != -1 && throttle != -1 || batchSize != -1 && secondThrottle != -1.0f){
                    long end;
                    long now = System.currentTimeMillis();
                    if (throttle != -1){
                        end = now + throttle;
                    }else {
                        end = now + (long)(secondThrottle * 1000);
                    }
                    while (msgs == null || (msgs.size()<batchSize && System.currentTimeMillis() < end)){
                        List<IMessage> tmp = null;
                        try {
                            now = System.currentTimeMillis();
                            if(end - now <= 0)
                                tmp = queue.take();
                            else
                                tmp = queue.poll(end - now, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            break;
                        }
                        if(tmp != null){
                            if(msgs == null)
                                msgs = new ArrayList<>(tmp);
                            else
                                msgs.addAll(tmp);
                        }
                    }
                }
                else {
                    long end;
                    long now = System.currentTimeMillis();
                    if (throttle != -1){
                        end = now + throttle;
                    }else {
                        end = now + (long)(secondThrottle * 1000);
                    }
                    while (msgs == null || System.currentTimeMillis() < end){
                        List<IMessage> tmp = null;
                        try {
                            if(end - now <= 0)
                                tmp = queue.take();
                            else
                                tmp = queue.poll(end - now, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e){
                            break;
                        }
                        if(tmp != null){
                            if(msgs == null)
                                msgs = tmp;
                            else
                                msgs.addAll(tmp);
                        }
                    }
                }
                if (msgs == null)
                    continue;
               if (batchHandler!=null)
                   batchHandler.batchHandler(msgs);
               else {
                   for (IMessage msg : msgs) {
                       handler.doEvent(msg);
                   }
               }
            }
        }
    }

    @Override
    protected boolean doReconnect(Site site) {
        if (!AbstractClient.ifUseBackupSite) {
            // not enable backupSite, use original logic
            String topicStr = site.host + ":" + site.port + "/" + site.tableName + "/" + site.actionName;
            Thread handlerLopper = null;
            synchronized (handlerLoppers) {
                if (!handlerLoppers.containsKey(topicStr))
                    throw new RuntimeException("Subscribe thread is not started");
                handlerLopper = handlerLoppers.get(topicStr);
            }
            handlerLopper.interrupt();
            try {
                subscribe(site.host, site.port, site.tableName, site.actionName, site.handler, site.msgId + 1, true, site.filter, site.deserializer, site.allowExistTopic, site.userName, site.passWord);
                Date d = new Date();
                DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                log.info(df.format(d) + " Successfully reconnected and subscribed " + site.host + ":" + site.port + "/" + site.tableName + "/" + site.actionName);
                return true;
            } catch (Exception ex) {
                Date d = new Date();
                DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                log.error(df.format(d) + " Unable to subscribe table. Will try again after 1 seconds." + site.host + ":" + site.port + "/" + site.tableName + "/" + site.actionName);
                ex.printStackTrace();
                return false;
            }
        } else {
            // enable backupSite, try to switch site and subscribe.
            synchronized (this) {
                log.info("ThreadedClient doReconnect: " + site.host + ":" + site.port);
                try {
                    System.out.println("doReconnect 尝试切换节点：" + site.host + ":" + site.port);
                    // System.out.println("site msg id: " +site.msgId);
                    subscribe(site.host, site.port, site.tableName, site.actionName, site.handler, site.msgId + 1, true, site.filter, site.deserializer, site.allowExistTopic, site.userName, site.passWord, false);
                    System.out.println("doReconnect 尝试切换节点成功：" + site.host + ":" + site.port);
                    String topicStr = site.host + ":" + site.port + "/" + site.tableName + "/" + site.actionName;
                    String curTopic = tableNameToTrueTopic.get(topicStr);
                    BlockingQueue<List<IMessage>> queue = queueManager.addQueue(curTopic);
                    queueManager.changeQueue(curTopic, lastQueue);
                    // System.out.println("切换后 handlerLoppers: " + handlerLoppers.get(topicStr).getName());
                    // System.out.println("切换成功后，handlerLoppers size: " + handlerLoppers.size());
                    Date d = new Date();
                    DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                    log.info(df.format(d) + " Successfully reconnected and subscribed " + site.host + ":" + site.port + "/" + site.tableName + "/" + site.actionName);
                    return true;
                } catch (Exception ex) {
                    Date d = new Date();
                    DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                    log.error(df.format(d) + " Unable to subscribe table. Will try again after 1 seconds." + site.host + ":" + site.port + "/" + site.tableName + "/" + site.actionName);
                    ex.printStackTrace();
                    return false;
                }
            }
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, String userName, String password) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, password, false);
        HandlerLopper handlerLopper = new HandlerLopper(queue, handler);
        handlerLopper.start();
        String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
        List<String> usr = Arrays.asList(userName, password);
        synchronized (handlerLoppers) {
            handlerLoppers.put(topicStr, handlerLopper);
            // users.put(topicStr, usr);
        }
    }

    /**
     * This internal subscribe method only use for when enable backupSite, try to switch site and subscribe.
     */
    protected void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, String userName, String password, boolean createSubInfo) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, password, false, createSubInfo);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, int batchSize, int throttle, String userName, String password) throws IOException {
        if(batchSize<=0)
            throw new IllegalArgumentException("BatchSize must be greater than zero");
        if(throttle<0)
            throw new IllegalArgumentException("Throttle must be greater than or equal to zero");
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, password, false);
        HandlerLopper handlerLopper = new HandlerLopper(queue, handler, batchSize, throttle == 0 ? -1 : throttle);
        handlerLopper.start();
        String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
        // List<String> usr = Arrays.asList(userName, password);
        synchronized (handlerLoppers) {
            handlerLoppers.put(topicStr, handlerLopper);
            // users.put(topicStr, usr);
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, int batchSize, int throttle, String userName, String password, List<String> backupSites, int resubTimeout, boolean subOnce) throws IOException {
        if(batchSize<=0)
            throw new IllegalArgumentException("BatchSize must be greater than zero");
        if(throttle<0)
            throw new IllegalArgumentException("Throttle must be greater than or equal to zero");
        if (resubTimeout < 0)
            // resubTimeout default: 100ms
            resubTimeout = 100;
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, password, false, backupSites, resubTimeout, subOnce);
        HandlerLopper handlerLopper = new HandlerLopper(queue, handler, batchSize, throttle == 0 ? -1 : throttle);
        handlerLopper.start();
        String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
        List<String> usr = Arrays.asList(userName, password);
        synchronized (handlerLoppers) {
            handlerLoppers.put(topicStr, handlerLopper);
            // users.put(topicStr, usr);
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, int batchSize, int throttle, String userName, String password, List<String> backupSites) throws IOException {
        if(batchSize<=0)
            throw new IllegalArgumentException("BatchSize must be greater than zero");
        if(throttle<0)
            throw new IllegalArgumentException("Throttle must be greater than or equal to zero");
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, password, false, backupSites, 100, false);
        HandlerLopper handlerLopper = new HandlerLopper(queue, handler, batchSize, throttle == 0 ? -1 : throttle);
        handlerLopper.start();
        String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
        List<String> usr = Arrays.asList(userName, password);
        synchronized (handlerLoppers) {
            handlerLoppers.put(topicStr, handlerLopper);
            // users.put(topicStr, usr);
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, int batchSize, float throttle, String userName, String password) throws IOException {
        if(batchSize<=0)
            throw new IllegalArgumentException("BatchSize must be greater than zero");
        if(throttle<0)
            throw new IllegalArgumentException("Throttle must be greater than or equal to zero");
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, password, false);
        HandlerLopper handlerLopper = new HandlerLopper(queue, handler, batchSize, throttle == 0.0f ? -1.0f : throttle);
        handlerLopper.start();
        String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
        List<String> usr = Arrays.asList(userName, password);
        synchronized (handlerLoppers) {
            handlerLoppers.put(topicStr, handlerLopper);
            // users.put(topicStr, usr);
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, BatchMessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, int batchSize, float throttle, String userName, String password) throws IOException {
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, batchSize, throttle, userName, password, false);
    }

    public void subscribe(String host, int port, String tableName, String actionName, BatchMessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, int batchSize, float throttle, String userName, String password, boolean msgAsTable) throws IOException {
        if(batchSize<=0)
            throw new IllegalArgumentException("BatchSize must be greater than zero");
        if(throttle<0)
            throw new IllegalArgumentException("Throttle must be greater than or equal to zero");
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, password, msgAsTable);
        HandlerLopper handlerLopper = new HandlerLopper(queue, handler, batchSize, throttle == 0.0f ? -1.0f : throttle);
        handlerLopper.start();
        String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
        List<String> usr = Arrays.asList(userName, password);
        synchronized (handlerLoppers) {
            handlerLoppers.put(topicStr, handlerLopper);
            // users.put(topicStr, usr);
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, BatchMessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, int batchSize, int throttle, String userName, String password) throws IOException {
        if(batchSize<=0)
            throw new IllegalArgumentException("BatchSize must be greater than zero");
        if(throttle<0)
            throw new IllegalArgumentException("Throttle must be greater than or equal to zero");
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, password, false);
        HandlerLopper handlerLopper = new HandlerLopper(queue, handler, batchSize, throttle == 0 ? -1 : throttle);
        handlerLopper.start();
        String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
        List<String> usr = Arrays.asList(userName, password);
        synchronized (handlerLoppers) {
            handlerLoppers.put(topicStr, handlerLopper);
            // users.put(topicStr, usr);
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic, String userName, String password) throws IOException {
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, null, allowExistTopic, userName, password);
    }

    public void subscribe(String host, int port, String tableName, String actionName, BatchMessageHandler batchMessageHandler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, int batchSize, float throttle) throws IOException{
        subscribe(host, port, tableName, actionName, batchMessageHandler, offset, reconnect, filter, deserializer, allowExistTopic, batchSize, throttle, "", "");
    }

    public void subscribe(String host, int port, String tableName, String actionName, BatchMessageHandler batchMessageHandler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, int batchSize, int throttle) throws IOException{
        subscribe(host, port, tableName, actionName, batchMessageHandler, offset, reconnect, filter, deserializer, allowExistTopic, batchSize, throttle, "", "");
    }

    public void subscribe(String host, int port, String tableName, String actionName, BatchMessageHandler batchMessageHandler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic, int batchSize, float throttle)throws IOException{
        subscribe(host, port, tableName, actionName, batchMessageHandler, offset, reconnect, filter, null, allowExistTopic, batchSize, throttle);
    }

    public void subscribe(String host, int port, String tableName, String actionName, BatchMessageHandler batchMessageHandler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic, int batchSize, int throttle)throws IOException{
        subscribe(host, port, tableName, actionName, batchMessageHandler, offset, reconnect, filter, null, allowExistTopic, batchSize, throttle);
    }

    public void subscribe(String host, int port, String tableName, String actionName, BatchMessageHandler batchMessageHandler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic, int batchSize, float throttle, String userName, String passWord)throws IOException{
        subscribe(host, port, tableName, actionName, batchMessageHandler, offset, reconnect, filter, null, allowExistTopic, batchSize, throttle, userName, passWord);
    }

    public void subscribe(String host, int port, String tableName, String actionName, BatchMessageHandler batchMessageHandler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic, int batchSize, int throttle, String userName, String passWord)throws IOException{
        subscribe(host, port, tableName, actionName, batchMessageHandler, offset, reconnect, filter, null, allowExistTopic, batchSize, throttle, userName, passWord);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, int batchSize, int throttle) throws IOException{
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, batchSize, throttle, "", "");
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, int batchSize, float throttle) throws IOException{
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

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic, int batchSize, float throttle)throws IOException{
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, null, allowExistTopic, batchSize, throttle);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic, int batchSize, int throttle, String userName, String passWord)throws IOException{
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, null, allowExistTopic, batchSize, throttle, userName, passWord);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic, int batchSize, float throttle, String userName, String passWord)throws IOException{
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
        if (!AbstractClient.ifUseBackupSite) {
            // original logic
            DBConnection dbConn = new DBConnection();
            List<String> tp = Arrays.asList(host, String.valueOf(port), tableName, actionName);
            List<String> usr = users.get(tp);
            String user = usr.get(0);
            String pwd = usr.get(1);
            if (!user.equals(""))
                dbConn.connect(host, port, user, pwd);
            else
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
                log.info("Successfully unsubscribed table " + fullTableName);
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
        } else {
            // use backBackSite
            String originHost = host;
            int originPort = port;

            synchronized (this) {
                DBConnection dbConn = new DBConnection();

                if (!currentSiteIndexMap.isEmpty()) {
                    Integer currentSiteIndex = currentSiteIndexMap.get(lastSuccessSubscribeTopic);
                    Site[] sites = trueTopicToSites.get(lastSuccessSubscribeTopic);
                    host = sites[currentSiteIndex].host;
                    port = sites[currentSiteIndex].port;
                }

                List<String> tp = Arrays.asList(host, String.valueOf(port), tableName, actionName);
                List<String> usr = users.get(tp);
                String user = usr.get(0);
                String pwd = usr.get(1);
                if (!user.equals(""))
                    dbConn.connect(host, port, user, pwd);
                else
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
                    // synchronized (tableNameToTrueTopic) {
                    topic = tableNameToTrueTopic.get(fullTableName);
                    // }
                    // synchronized (trueTopicToSites) {
                    Site[] sites = trueTopicToSites.get(topic);
                    if (sites == null || sites.length == 0)
                        ;
                    for (int i = 0; i < sites.length; i++)
                        sites[i].closed = true;
                    // }
                    // synchronized (queueManager) {
                    queueManager.removeQueue(lastBackupSiteTopic);
                    // }
                    log.info("Successfully unsubscribed table " + fullTableName);
                } catch (Exception ex) {
                    throw ex;
                } finally {
                    dbConn.close();
                    String topicStr = originHost + ":" + originPort + "/" + tableName + "/" + actionName;
                    HandlerLopper handlerLopper = null;
                    // synchronized (handlerLoppers) {
                    handlerLopper = handlerLoppers.get(topicStr);
                    handlerLoppers.remove(topicStr);
                    handlerLopper.interrupt();
                    // }
                }
            }
        }
    }

    public void close(){
        synchronized (handlerLoppers) {
            Iterator<HandlerLopper> it = handlerLoppers.values().iterator();
            while (it.hasNext()) {
                it.next().interrupt();
            }
            handlerLoppers.clear();
        }
        if(pThread != null)
            pThread.interrupt();
        isClose_ = true;
    }
}
