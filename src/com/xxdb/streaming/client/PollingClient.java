package com.xxdb.streaming.client;


import com.xxdb.*;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicString;
import com.xxdb.data.Entity;
import com.xxdb.data.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class PollingClient extends AbstractClient {
    TopicPoller topicPoller = null;
    // private HashMap<List<String>, List<String>> users = new HashMap<>();

    private static final Logger log = LoggerFactory.getLogger(PollingClient.class);

    public PollingClient() throws SocketException {
        super(0);
    }

    public PollingClient(int subscribePort) throws SocketException {
        super(subscribePort);
    }

    public PollingClient(String subscribeHost, int subscribePort) throws SocketException {
        super(subscribeHost, subscribePort);
    }

    @Override
    protected boolean doReconnect(Site site) {
        if (!AbstractClient.ifUseBackupSite) {
            // not enable backupSite, use original logic.
            try {
                log.info("PollingClient doReconnect: " + site.host + ":" + site.port);
                Thread.sleep(1000);
                BlockingQueue<List<IMessage>> queue = subscribeInternal(site.host, site.port, site.tableName, site.actionName, (MessageHandler) null, site.msgId + 1, true, site.filter, site.deserializer, site.allowExistTopic, site.userName, site.passWord, site.msgAstable);
                log.info("Successfully reconnected and subscribed " + site.host + ":" + site.port + ":" + site.tableName);
                topicPoller.setQueue(queue);
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
            try {
                log.info("PollingClient doReconnect: " + site.host + ":" + site.port);
                Thread.sleep(1000);
                subscribe(site.host, site.port, site.tableName, site.actionName, (MessageHandler) null, site.msgId + 1, true, site.filter, site.deserializer, site.allowExistTopic, site.userName, site.passWord, site.msgAstable, false);
                String topicStr = site.host + ":" + site.port + "/" + site.tableName + "/" + site.actionName;
                String curTopic = tableNameToTrueTopic.get(topicStr);
                BlockingQueue<List<IMessage>> queue = queueManager.addQueue(curTopic);
                queueManager.changeQueue(curTopic, lastQueue);

                log.info("Successfully reconnected and subscribed " + site.host + ":" + site.port + ":" + site.tableName);
                topicPoller.setQueue(lastQueue);
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
                log.error("Unable to subscribe table. Will try again after 1 seconds.");
                ex.printStackTrace();
                return false;
            }
        }
    }

    protected void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, String userName, String password, boolean msgAsTable, boolean createSubInfo) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, password, false, createSubInfo);
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, String userName, String passWord) throws IOException {
        return subscribe(host, port, tableName, actionName, offset, reconnect, filter, deserializer, userName, passWord, false);
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, String userName, String passWord, boolean msgAsTable) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, (MessageHandler) null, offset, reconnect, filter, deserializer, false, userName, passWord, msgAsTable);
        List<String> tp = Arrays.asList(host, String.valueOf(port), tableName, actionName);
        List<String> usr = Arrays.asList(userName, passWord);
        // users.put(tp, usr);
        topicPoller = new TopicPoller(queue);
        return topicPoller;
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, String userName, String passWord, boolean msgAsTable, List<String> backupSites, int resubscribeInterval, boolean subOnce) throws IOException {
        if (resubscribeInterval < 0)
            // resubscribeInterval default: 100ms
            resubscribeInterval = 100;

        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, (MessageHandler) null, offset, reconnect, filter, deserializer, false, userName, passWord, msgAsTable, backupSites, resubscribeInterval, subOnce);
        topicPoller = new TopicPoller(queue);
        return topicPoller;
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, String userName, String passWord, boolean msgAsTable, List<String> backupSites) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, (MessageHandler) null, offset, reconnect, filter, deserializer, false, userName, passWord, msgAsTable, backupSites, 100, false);
        topicPoller = new TopicPoller(queue);
        return topicPoller;
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, String userName, String passWord) throws IOException {
        return subscribe(host, port, tableName, actionName, offset, reconnect, filter, null, userName, passWord);
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer)throws IOException{
        return subscribe(host, port, tableName, actionName, offset, reconnect, filter, deserializer, "", "");
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter)throws IOException{
        return subscribe(host, port, tableName, actionName, offset, reconnect, filter, null);
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect) throws IOException {
        return subscribe(host, port, tableName, actionName, offset, reconnect, null);
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, Vector filter) throws IOException {
        return subscribe(host, port, tableName, actionName, offset, false, filter);
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset) throws IOException {
        return subscribe(host, port, tableName, actionName, offset, false);
    }

    public TopicPoller subscribe(String host, int port, String tableName, long offset) throws IOException {
        return subscribe(host, port, tableName, DEFAULT_ACTION_NAME, offset);
    }

    public TopicPoller subscribe(String host, int port, String tableName, long offset, boolean reconnect) throws IOException {
        return subscribe(host, port, tableName, DEFAULT_ACTION_NAME, offset, reconnect);
    }

    public TopicPoller subscribe(String host, int port, String tableName) throws IOException {
        return subscribe(host, port, tableName, -1);
    }

    public TopicPoller subscribe(String host, int port, String tableName, boolean reconnect) throws IOException {
        return subscribe(host, port, tableName, -1, reconnect);
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName) throws IOException {
        return subscribe(host, port, tableName, actionName, -1);
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName, boolean reconnect) throws IOException {
        return subscribe(host, port, tableName, actionName, -1, reconnect);
    }

    public void unsubscribe(String host, int port, String tableName, String actionName) throws IOException {
        unsubscribeInternal(host, port, tableName, actionName);
    }

    public void unsubscribe(String host, int port, String tableName) throws IOException {
        unsubscribeInternal(host, port, tableName, DEFAULT_ACTION_NAME);
    }

    @Override
    protected void unsubscribeInternal(String host, int port, String tableName, String actionName) throws IOException {
        synchronized (this) {
            DBConnection dbConn = new DBConnection();
            if (!currentSiteIndexMap.isEmpty()) {
                String topic = tableNameToTrueTopic.get( host + ":" + port + "/" + tableName + "/" + actionName);
                Integer currentSiteIndex = currentSiteIndexMap.get(topic);
                Site[] sites = trueTopicToSites.get(topic);
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
                        host = matchedInfo.getLeaderIp();
                        port = matchedInfo.getLeaderPort();
                    }
                }
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
            }
        }
    }
}
