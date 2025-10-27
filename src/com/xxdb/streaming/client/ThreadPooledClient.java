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
                Object[] hostPort = new Object[2];
                if (getNewLeader(ex.getMessage(), hostPort)) {
                    log.warn("In reconnect: Got NotLeaderException, switch to leader node [" + hostPort[0] + ":" + hostPort[1] + "] for subscription");
                    haStreamTableInfo.add(new HAStreamTableInfo(site.host, site.port, site.tableName, site.actionName, (String) hostPort[0], (Integer) hostPort[1]));
                    site.host = (String) hostPort[0];
                    site.port = (Integer) hostPort[1];
                } else {
                    log.error("Unable to subscribe table. Will try again after 1 seconds.");
                    ex.printStackTrace();
                }
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

                // after last threadPool completely shutdown, create a new threadPool（because ExecutorService lifecycle is unidirectional)
                if (threadPool.isTerminated())
                    threadPool = Executors.newFixedThreadPool(threadCount);

                log.info("Successfully reconnected and subscribed " + site.host + ":" + site.port + "/" + site.tableName + site.actionName);
                return true;
            } catch (Exception ex) {
                Object[] hostPort = new Object[2];
                if (getNewLeader(ex.getMessage(), hostPort)) {
                    log.warn("In reconnect: Got NotLeaderException, switch to leader node [" + hostPort[0] + ":" + hostPort[1] + "] for subscription");
                    haStreamTableInfo.add(new HAStreamTableInfo(site.host, site.port, site.tableName, site.actionName, (String) hostPort[0], (Integer) hostPort[1]));
                    site.host = (String) hostPort[0];
                    site.port = (Integer) hostPort[1];
                } else {
                    log.error("Unable to subscribe table. Will try again after 1 seconds.");
                    ex.printStackTrace();
                }
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

        if (!haStreamTableInfo.isEmpty()) {
            synchronized (haStreamTableInfo) {
                HAStreamTableInfo matchedInfo = null;
                for (HAStreamTableInfo info : haStreamTableInfo) {
                    if (info.getFollowIp().equals(host) &&
                            info.getFollowPort() == port &&
                            info.getTableName().equals(tableName) &&
                            info.getActionName().equals(actionName)) {
                        matchedInfo = info;
                        break;
                    }
                }

                if (matchedInfo != null) {
                    host = matchedInfo.getLeaderIp();
                    port = matchedInfo.getLeaderPort();
                }
            }
        }

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

        if (!haStreamTableInfo.isEmpty()) {
            synchronized (haStreamTableInfo) {
                HAStreamTableInfo matchedInfo = null;
                for (HAStreamTableInfo info : haStreamTableInfo) {
                    if (info.getFollowIp().equals(host) &&
                            info.getFollowPort() == port &&
                            info.getTableName().equals(tableName) &&
                            info.getActionName().equals(actionName)) {
                        matchedInfo = info;
                        break;
                    }
                }

                if (matchedInfo != null) {
                    host = matchedInfo.getLeaderIp();
                    port = matchedInfo.getLeaderPort();
                }
            }
        }

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
        String originalFullTableName = host + ":" + port + "/" + tableName + "/" + actionName;
        log.debug("Starting unsubscribe process for " + originalFullTableName);

        if (!ifUseBackupSite) {
            // original logic:
            DBConnection dbConn = new DBConnection();

            if (!haStreamTableInfo.isEmpty()) {
                synchronized (haStreamTableInfo) {
                    HAStreamTableInfo matchedInfo = null;
                    for (HAStreamTableInfo info : haStreamTableInfo) {
                        if (info.getFollowIp().equals(host) &&
                                info.getFollowPort() == port &&
                                info.getTableName().equals(tableName) &&
                                info.getActionName().equals(actionName)) {
                            matchedInfo = info;
                            break;
                        }
                    }

                    if (matchedInfo != null) {
                        log.debug("Found HA matched info, switching from " + host + ":" + port + " to leader " + matchedInfo.getLeaderIp() + ":" + matchedInfo.getLeaderPort());
                        host = matchedInfo.getLeaderIp();
                        port = matchedInfo.getLeaderPort();
                    }
                }
            }

            List<String> tp = Arrays.asList(host, String.valueOf(port), tableName, actionName);
            List<String> usr = users.get(tp);
            String user = usr.get(0);
            String pwd = usr.get(1);
            log.debug("Connecting to server " + host + ":" + port + " to send unsubscribe signal for " + originalFullTableName);
            if (!user.equals(""))
                dbConn.connect(host, port, user, pwd);
            else
                dbConn.connect(host, port);
            log.debug("Connected to server " + host + ":" + port + " successfully");
            try {
                // Get topic and mark sites as closed BEFORE sending stopPublishTable
                // This prevents MessageParser from triggering reconnect when it receives EOF
                String topic;
                String fullTableName = host + ":" + port + "/" + tableName + "/" + actionName;
                synchronized (tableNameToTrueTopic) {
                    topic = tableNameToTrueTopic.get(fullTableName);
                    log.debug("Retrieved topic from tableNameToTrueTopic: " + topic + " for " + fullTableName);
                }
                synchronized (trueTopicToSites) {
                    Site[] sites = trueTopicToSites.get(topic);
                    if (sites == null || sites.length == 0) {
                        log.warn("No sites found for topic: " + topic);
                    } else {
                        log.info("Marking " + sites.length + " site(s) as closed for topic: " + topic + " BEFORE sending stopPublishTable");
                        for (int i = 0; i < sites.length; i++) {
                            sites[i].closed = true;
                            log.debug("Site " + i + " marked as closed: " + sites[i].host + ":" + sites[i].port);
                        }
                    }
                }

                String localIP = this.listeningHost;
                if(localIP.equals(""))
                    localIP = dbConn.getLocalAddress().getHostAddress();
                List<Entity> params = new ArrayList<Entity>();
                params.add(new BasicString(localIP));
                params.add(new BasicInt(this.listeningPort));
                params.add(new BasicString(tableName));
                params.add(new BasicString(actionName));

                log.debug("Sending stopPublishTable command with params: localIP=" + localIP + ", listeningPort=" + this.listeningPort + ", tableName=" + tableName + ", actionName=" + actionName);
                dbConn.run("stopPublishTable", params);
                log.debug("stopPublishTable command executed successfully for " + originalFullTableName);
                synchronized (queueManager) {
                    queueManager.removeQueue(topic);
                    log.debug("Queue removed from queueManager for topic: " + topic);
                }

                log.debug("Successfully unsubscribed table " + fullTableName);
            } catch (Exception ex) {
                log.error("Error occurred during unsubscribe for " + originalFullTableName, ex);
                throw ex;
            } finally {
                dbConn.close();
                log.debug("DBConnection closed for " + originalFullTableName);

                String topicStr = host + ":" + port + "/" + tableName + "/" + actionName;
                synchronized (queueHandlers){
                    queueHandlers.remove(topicStr);
                }
                log.debug("Unsubscribe process completed for " + originalFullTableName);
            }
        } else {
            // use backBackSite
            log.debug("Using backupSite mode for unsubscribe");
            String originHost = host;
            int originPort = port;

            synchronized (this) {
                DBConnection dbConn = new DBConnection();
                if (!currentSiteIndexMap.isEmpty()) {
                    String topic = tableNameToTrueTopic.get( host + ":" + port + "/" + tableName + "/" + actionName);
                    Integer currentSiteIndex = currentSiteIndexMap.get(topic);
                    Site[] sites = trueTopicToSites.get(topic);
                    log.debug("Switching to current site index " + currentSiteIndex + ": " + sites[currentSiteIndex].host + ":" + sites[currentSiteIndex].port);
                    host = sites[currentSiteIndex].host;
                    port = sites[currentSiteIndex].port;
                }

                if (!haStreamTableInfo.isEmpty()) {
                    synchronized (haStreamTableInfo) {
                        HAStreamTableInfo matchedInfo = null;
                        for (HAStreamTableInfo info : haStreamTableInfo) {
                            if (info.getFollowIp().equals(host) &&
                                    info.getFollowPort() == port &&
                                    info.getTableName().equals(tableName) &&
                                    info.getActionName().equals(actionName)) {
                                matchedInfo = info;
                                break;
                            }
                        }

                        if (matchedInfo != null) {
                            log.debug("Found HA matched info, switching from " + host + ":" + port + " to leader " + matchedInfo.getLeaderIp() + ":" + matchedInfo.getLeaderPort());
                            host = matchedInfo.getLeaderIp();
                            port = matchedInfo.getLeaderPort();
                        }
                    }
                }

                List<String> tp = Arrays.asList(host, String.valueOf(port), tableName, actionName);
                List<String> usr = users.get(tp);
                String user = usr.get(0);
                String pwd = usr.get(1);

                log.debug("Connecting to server " + host + ":" + port + " to send unsubscribe signal (backupSite mode) for " + originalFullTableName);
                if (!user.equals(""))
                    dbConn.connect(host, port, user, pwd);
                else
                    dbConn.connect(host, port);

                log.debug("Connected to server " + host + ":" + port + " successfully (backupSite mode)");

                try {
                    // Get topic and mark sites as closed BEFORE sending stopPublishTable
                    // This prevents MessageParser from triggering reconnect when it receives EOF
                    String topic;
                    String fullTableName = host + ":" + port + "/" + tableName + "/" + actionName;
                    topic = tableNameToTrueTopic.get(fullTableName);
                    log.debug("Retrieved topic from tableNameToTrueTopic: " + topic + " for " + fullTableName);

                    Site[] sites = trueTopicToSites.get(topic);
                    if (sites == null || sites.length == 0) {
                        log.warn("No sites found for topic: " + topic);
                    } else {
                        log.debug("Marking " + sites.length + " site(s) as closed for topic: " + topic + " BEFORE sending stopPublishTable (backupSite mode)");
                        for (int i = 0; i < sites.length; i++) {
                            sites[i].closed = true;
                            log.debug("Site " + i + " marked as closed: " + sites[i].host + ":" + sites[i].port);
                        }
                    }

                    String localIP = this.listeningHost;
                    if(localIP.equals(""))
                        localIP = dbConn.getLocalAddress().getHostAddress();
                    List<Entity> params = new ArrayList<Entity>();
                    params.add(new BasicString(localIP));
                    params.add(new BasicInt(this.listeningPort));
                    params.add(new BasicString(tableName));
                    params.add(new BasicString(actionName));

                    log.debug("Sending stopPublishTable command (backupSite mode) with params: localIP=" + localIP + ", listeningPort=" + this.listeningPort + ", tableName=" + tableName + ", actionName=" + actionName);
                    dbConn.run("stopPublishTable", params);
                    log.debug("stopPublishTable command executed successfully (backupSite mode) for " + originalFullTableName);

                    queueManager.removeQueue(topic);
                    log.debug("Queue removed from queueManager for topic: " + topic);

                    // init backupSites related params.
                    if (AbstractClient.ifUseBackupSite) {
                        log.debug("Resetting backupSite related parameters");
                        AbstractClient.ifUseBackupSite = false;
                        AbstractClient.subOnce = false;
                        AbstractClient.resubscribeInterval = 100;
                    }
                    log.debug("Successfully unsubscribed table " + fullTableName);
                } catch (Exception ex) {
                    log.error("Error occurred during unsubscribe (backupSite mode) for " + originalFullTableName, ex);
                    throw ex;
                } finally {
                    dbConn.close();
                    log.debug("DBConnection closed (backupSite mode) for " + originalFullTableName);

                    String topicStr = originHost + ":" + originPort + "/" + tableName + "/" + actionName;
                    synchronized (queueHandlers){
                        queueHandlers.remove(topicStr);
                    }
                    log.debug("Unsubscribe process completed (backupSite mode) for " + originalFullTableName);
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
