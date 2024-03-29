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
    private HashMap<List<String>, List<String>> users = new HashMap<>();

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
        try {
            Thread.sleep(1000);
            BlockingQueue<List<IMessage>> queue = subscribeInternal(site.host, site.port, site.tableName, site.actionName, (MessageHandler) null, site.msgId + 1, true, site.filter, site.deserializer, site.allowExistTopic, site.userName, site.passWord, site.msgAstable);
            log.info("Successfully reconnected and subscribed " + site.host + ":" + site.port + ":" + site.tableName);
            topicPoller.setQueue(queue);
            return true;
        } catch (Exception ex) {
            log.error("Unable to subscribe table. Will try again after 1 seconds.");
            ex.printStackTrace();
            return false;
        }
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, String userName, String passWord) throws IOException {
        return subscribe(host, port, tableName, actionName, offset, reconnect, filter, deserializer, userName, passWord, false);
    }

    public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, StreamDeserializer deserializer, String userName, String passWord, boolean msgAsTable) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, (MessageHandler) null, offset, reconnect, filter, deserializer, false, userName, passWord, msgAsTable);
        List<String> tp = Arrays.asList(host, String.valueOf(port), tableName, actionName);
        List<String> usr = Arrays.asList(userName, passWord);
        users.put(tp, usr);
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
        }
        return;
    }
}
