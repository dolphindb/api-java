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
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;


public class ThreadPooledClient extends AbstractClient {
    private static int CORES = Runtime.getRuntime().availableProcessors();
    private ExecutorService threadPool;
    // private HashMap<String, List<String>> users = new HashMap<>();
    private Object lock = new Object();
    private int threadCount = -1;

    private static final Logger log = LoggerFactory.getLogger(ThreadPooledClient.class);

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
        this("", DEFAULT_PORT, CORES);
    }

    public ThreadPooledClient(int threadCount) throws SocketException {
        this("", 0, threadCount);
    }

    public ThreadPooledClient(int subscribePort, int threadCount) throws SocketException {
        this("", subscribePort, threadCount);
    }

    public ThreadPooledClient(String subscribeHost, int subscribePort, int threadCount) throws SocketException {
        super(subscribeHost, subscribePort);
        if (threadCount <= 0)
            throw new RuntimeException("The 'threadCount' parameter cannot be less than or equal to zero.");

        this.threadCount = threadCount;
        threadPool = Executors.newFixedThreadPool(threadCount);

        new Thread() {
            private LinkedList<IMessage> backlog = new LinkedList<>();

            private boolean fillBacklog() {
                boolean filled = false;
                synchronized (queueHandlers) {
                    for (String topic : queueHandlers.keySet()) {
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
                while (!fillBacklog()) {
                    if (count > 0)
                        Thread.yield();
                    else
                        // Pause for 1 millisecond
                        LockSupport.parkNanos(1000 * 1000);
                    count = count - 1;
                }
            }

            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
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

    class HandlerRunner implements Runnable {
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

    protected boolean doReconnect(Site site) {
        if (!AbstractClient.ifUseBackupSite) {
            // not enable backupSite, use original logic
            threadPool.shutdownNow();
            try {
                Thread.sleep(1000);
                subscribe(site.host, site.port, site.tableName, site.actionName, site.handler, site.msgId + 1, true, site.filter, site.deserializer, site.allowExistTopic, site.userName, site.passWord);
                log.info("Successfully reconnected and subscribed " + site.host + ":" + site.port + "/" + site.tableName + site.actionName);
                return true;
            } catch (Exception ex) {
                log.error("Unable to subscribe table. Will try again after 1 seconds.");
                ex.printStackTrace();
                return false;
            }
        } else {
            // enable backupSite, try to switch site and subscribe.
            log.info("ThreadPooledClient doReconnect: " + site.host + ":" + site.port);
            try {
                Thread.sleep(1000);
                backupSitesSubscribeInternal(site.host, site.port, site.tableName, site.actionName, site.handler, site.msgId + 1, true, site.filter, site.deserializer, site.allowExistTopic, site.userName, site.passWord, false);
                String topicStr = site.host + ":" + site.port + "/" + site.tableName + "/" + site.actionName;
                String curTopic = tableNameToTrueTopic.get(topicStr);
                BlockingQueue<List<IMessage>> queue = queueManager.addQueue(curTopic);
                queueManager.changeQueue(curTopic, lastQueue);

                QueueHandlerBinder queueHandlerBinder = queueHandlers.get(lastBackupSiteTopic);
                queueHandlers.put(curTopic, queueHandlerBinder);

                // shutdown last threadPool, and wait all tasks to finish.
                threadPool.shutdown();
                try {
                    if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                        threadPool.shutdownNow();
                        if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                            log.error("last threadPool did not terminate.");
                        }
                    }
                } catch (InterruptedException ie) {
                    threadPool.shutdownNow();
                    Thread.currentThread().interrupt();
                }

                // after last threadPool completely shutdown，create a new threadPool（because ExecutorService lifecycle is unidirectional）.
                if (threadPool.isTerminated())
                    threadPool = Executors.newFixedThreadPool(threadCount);

                log.info("Successfully reconnected and subscribed " + site.host + ":" + site.port + "/" + site.tableName + site.actionName);
                return true;
            } catch (Exception ex) {
                log.error("Unable to subscribe table. Will try again after 1 seconds.");
                ex.printStackTrace();
                return false;
            }
        }
    }

    protected void backupSitesSubscribeInternal(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, String userName, String password, boolean createSubInfo) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, password, false, createSubInfo);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, String userName, String passWord) throws IOException {
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, passWord, false);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, String userName, String passWord, boolean msgAsTable) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, passWord, msgAsTable);
        String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
        List<String> usr = Arrays.asList(userName, passWord);
        synchronized (queueHandlers) {
            queueHandlers.put(tableNameToTrueTopic.get(topicStr), new QueueHandlerBinder(queue, handler));
            // users.put(topicStr, usr);
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, String userName, String passWord, boolean msgAsTable, List<String> backupSites, int resubscribeInterval, boolean subOnce) throws IOException {
        if (resubscribeInterval < 0)
            // resubscribeInterval default: 100ms
            resubscribeInterval = 100;

        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, passWord, msgAsTable, backupSites, resubscribeInterval, subOnce);
        String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
        synchronized (queueHandlers) {
            queueHandlers.put(tableNameToTrueTopic.get(topicStr), new QueueHandlerBinder(queue, handler));
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, String userName, String passWord, boolean msgAsTable, List<String> backupSites) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, passWord, msgAsTable, backupSites, 100, false);
        String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
        synchronized (queueHandlers) {
            queueHandlers.put(tableNameToTrueTopic.get(topicStr), new QueueHandlerBinder(queue, handler));
        }
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic, String userName, String passWord) throws IOException{
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, null, allowExistTopic, userName, passWord);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic) throws IOException{
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, "", "");
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, boolean allowExistTopic) throws IOException {
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, null, allowExistTopic);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer)throws IOException{
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, false);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect) throws IOException {
        subscribe(host, port, tableName, actionName, handler, offset, reconnect, null, null, false);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, Vector filter) throws IOException {
        subscribe(host, port, tableName, actionName, handler, offset, false, filter, null, false);
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

    @Override
    protected void unsubscribeInternal(String host, int port, String tableName, String actionName) throws IOException {
        if (!ifUseBackupSite) {
            // original logic:
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
                QueueHandlerBinder queueHandler =null;
                synchronized (queueHandlers){
                    queueHandler = queueHandlers.get(topicStr);
                    queueHandlers.remove(topicStr);
                }
            }
        } else {
            // use backBackSite
            String originHost = host;
            int originPort = port;

            synchronized (this) {
                DBConnection dbConn = new DBConnection();
                if (!currentSiteIndexMap.isEmpty()) {
                    String topic = tableNameToTrueTopic.get( host + ":" + port + "/" + tableName + "/" + actionName);
                    Integer currentSiteIndex = currentSiteIndexMap.get(topic);
                    Site[] sites = trueTopicToSites.get(topic);
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
                    topic = tableNameToTrueTopic.get(fullTableName);

                    Site[] sites = trueTopicToSites.get(topic);
                    if (sites == null || sites.length == 0)
                        ;
                    for (int i = 0; i < sites.length; i++)
                        sites[i].closed = true;

                    queueManager.removeQueue(topic);

                    // init backupSites related params.
                    if (AbstractClient.ifUseBackupSite) {
                        AbstractClient.ifUseBackupSite = false;
                        AbstractClient.subOnce = false;
                        AbstractClient.resubscribeInterval = 100;
                    }
                    log.info("Successfully unsubscribed table " + fullTableName);
                } catch (Exception ex) {
                    throw ex;
                } finally {
                    dbConn.close();
                    String topicStr = originHost + ":" + originPort + "/" + tableName + "/" + actionName;
                    QueueHandlerBinder queueHandler =null;
                    synchronized (queueHandlers){
                        queueHandler = queueHandlers.get(topicStr);
                        queueHandlers.remove(topicStr);
                    }
                }
            }
        }
    }

    public  void close(){
        synchronized (queueHandlers) {
            queueHandlers = null;
        }
        threadPool.shutdownNow();
        if(pThread != null)
            pThread.interrupt();
        isClose_ = true;
    }
}
