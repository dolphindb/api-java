package com.xxdb.streaming.client;


import java.io.IOException;
import java.net.SocketException;
import java.util.*;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import com.xxdb.DBConnection;
import com.xxdb.comm.SqlStdEnum;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.data.Void;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractClient implements MessageDispatcher {
    protected static final int DEFAULT_PORT = 8849;
    protected static final String DEFAULT_HOST = "localhost";
    protected static final String DEFAULT_ACTION_NAME = "javaStreamingApi";
    protected ConcurrentHashMap<String, ReconnectItem> reconnectTable = new ConcurrentHashMap<String, ReconnectItem>();

    protected int listeningPort;
    protected String listeningHost = "";
    protected QueueManager queueManager = new QueueManager();
    protected ConcurrentHashMap<String, List<IMessage>> messageCache = new ConcurrentHashMap<>();
    protected HashMap<String, String> tableNameToTrueTopic = new HashMap<>();
    protected HashMap<String, String> HATopicToTrueTopic = new HashMap<>();
    protected HashMap<String, Boolean> hostEndian = new HashMap<>();
    protected Thread pThread;
    protected ConcurrentHashMap<String, Site[]> trueTopicToSites = new ConcurrentHashMap<>();
    protected CopyOnWriteArraySet<String> waitReconnectTopic = new CopyOnWriteArraySet<>();
    protected Map<String, StreamDeserializer> subInfos_ = new HashMap<>();
    protected HashMap<List<String>, List<String>> users = new HashMap<>();
    protected boolean isClose_ = false;
    protected LinkedBlockingQueue<DBConnection> connList = new LinkedBlockingQueue<>();
    protected Map<String, Integer> currentSiteIndexMap = new ConcurrentHashMap<>();

    private Daemon daemon = null;

    private static final Logger log = LoggerFactory.getLogger(AbstractClient.class);

    class ReconnectItem {
        /**
         * 0: connected and received message schema
         * 1: not requested yet
         * 2: requested, but not received message schema yet
         */
        private int reconnectState;
        private long lastReconnectTimestamp;
        private List<String> topics;

        public ReconnectItem(int v, long t) {
            reconnectState = v;
            lastReconnectTimestamp = t;
            topics = new ArrayList<>();
        }

        public void setState(int v) {
            reconnectState = v;
        }

        public int getState() {
            return reconnectState;
        }

        public void setTimestamp(long v) {
            lastReconnectTimestamp = v;
        }

        public long getTimestamp() {
            return lastReconnectTimestamp;
        }

        public void putTopic(String topic) {
            if (this.topics == null) {
                topics = new ArrayList<>();
                topics.add(topic);
            } else {
                if (!this.topics.contains(topics)) {
                    this.topics.add(topic);
                }
            }
        }

        public List<String> getTopics() {
            return topics;
        }
    }


    public void setNeedReconnect(String topic, int v) {
        if (topic.equals("")) return;
        String site = topic.substring(0, topic.indexOf("/"));
        Set<String> keys = reconnectTable.keySet();
        if (!keys.contains(site)) {
            ReconnectItem item = new ReconnectItem(v, System.currentTimeMillis());
            item.putTopic(topic);
            reconnectTable.put(site, item);
        } else {
            ReconnectItem item = reconnectTable.get(site);
            item.setState(v);
            item.setTimestamp(System.currentTimeMillis());
            item.putTopic(topic);
        }
    }

    public int getNeedReconnect(String site) {
        ReconnectItem item = this.reconnectTable.get(site);
        if (item != null)
            return item.getState();
        else
            return 0;
    }

    public long getReconnectTimestamp(String site) {
        ReconnectItem item = this.reconnectTable.get(site);
        if (item != null)
            return item.getTimestamp();
        else
            return 0;
    }

    public void setReconnectTimestamp(String site, long v) {
        ReconnectItem item = this.reconnectTable.get(site);
        if (item != null)
            item.setTimestamp(v);
    }

    public List<String> getAllTopicsBySite(String site) {
        List<String> re = new ArrayList<>();
        for (String topic : this.trueTopicToSites.keySet()) {
            String s = topic.substring(0, topic.indexOf("/"));
            if (s.equals(site)) {
                re.add(topic);
            }
        }
        return re;
    }

    public Set<String> getAllReconnectTopic() {
        return waitReconnectTopic;
    }

    public List<String> getAllReconnectSites() {
        List<String> re = new ArrayList<>();
        for (String site : reconnectTable.keySet()) {
            ReconnectItem item = reconnectTable.get(site);
            if (item.getState() > 0) {
                re.add(site);
            }
        }
        return re;
    }

    public Site getSiteByName(String site) {
        List<String> topics = this.getAllTopicsBySite(site);
        if (topics.size() > 0) {
            Site[] sites = trueTopicToSites.get(topics.get(0));
            if (sites.length > 0)
                return getActiveSite(sites);
        }
        return null;
    }

    public Map<String, StreamDeserializer> getSubInfos(){
        return subInfos_;
    }

    abstract protected boolean doReconnect(Site site);

    public void setMsgId(String topic, long msgId) {
        synchronized (trueTopicToSites) {
            Site[] sites = trueTopicToSites.get(topic);
            if (sites == null || sites.length == 0)
                return;
            if (sites.length == 1)
                sites[0].msgId = msgId;
        }
    }

    public boolean tryReconnect(String topic) {
        if (currentSiteIndexMap.isEmpty()) {
            synchronized (reconnectTable) {
                topic = HATopicToTrueTopic.get(topic);
                queueManager.removeQueue(topic);
                Site[] sites = null;
                synchronized (trueTopicToSites) {
                    sites = trueTopicToSites.get(topic);
                }
                if (sites == null || sites.length == 0)
                    return false;
                if (sites.length == 1) {
                    if (!sites[0].reconnect)
                        return false;
                }
                Site site = getActiveSite(sites);
                if (site != null) {
                    if (!doReconnect(site)) {
                        waitReconnectTopic.add(topic);
                        return false;
                    } else {
                        waitReconnectTopic.remove(topic);
                        return true;
                    }
                } else {
                    return false;
                }
            }
        } else {
            // if set backupSites
            synchronized (reconnectTable) {
                topic = HATopicToTrueTopic.get(topic);
                queueManager.removeQueue(topic);
                Site[] sites;
                synchronized (trueTopicToSites) {
                    sites = trueTopicToSites.get(topic);
                }
                if (sites == null || sites.length == 0)
                    return false;
                if (sites.length == 1) {
                    if (!sites[0].reconnect)
                        return false;
                }

                boolean reconnected = false;
                for (int i = 0; i < sites.length; i ++) {
                    Integer currentSiteIndex = currentSiteIndexMap.get(topic);
                    if (currentSiteIndexMap.get(topic) != -1)
                        i = currentSiteIndex;
                    Site site = sites[i];
                    boolean siteReconnected = false;
                    for (int attempt = 0; attempt < 2; attempt++) {
                        // try twice every site
                        if (doReconnect(site)) {
                            siteReconnected = true;
                            // if site current connect successfully, break
                            break;
                        }
                    }
                    if (siteReconnected) {
                        reconnected = true;
                        currentSiteIndexMap.put(topic, i);
                        // if current site connect successfully, no tryReconnect other site, break
                        break;
                    }
                }

                if (!reconnected) {
                    waitReconnectTopic.add(topic);
                    return false;
                } else {
                    waitReconnectTopic.remove(topic);
                    return true;
                }
            }
        }
    }

    private Site getActiveSite(Site[] sites) {
        int siteId = 0;
        int siteNum = sites.length;

        while (true) {
            Site site = sites[siteId];
            siteId = (siteId + 1) % siteNum;
            try {
                DBConnection conn = new DBConnection();
                conn.connect(site.host, site.port);
                try {
                    conn.run("1");
                    return site;
                } catch (IOException ioex) {
                    throw ioex;
                } finally {
                    conn.close();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            try {
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private int getVersionNumber(String ver) {
        try {
            String[] s = ver.split(" ");
            if (s.length >= 2) {
                String vernum = s[0].replace(".", "");
                return Integer.parseInt(vernum);
            }
        } 
        catch (Exception ex) {}
        return 0;
    }

    public void activeCloseConnection(Site site) {
        try {
            DBConnection conn = new DBConnection();
            conn.connect(site.host, site.port);

            try {
                List<Entity> params = new ArrayList<>();
                String actionName = site.actionName;
                String tableName = site.tableName;
                params.add(new BasicString(actionName));
                params.add(new BasicString(tableName));

                BasicString version = (BasicString) conn.run("version()");
                int verNum = getVersionNumber(version.getString());
                if (verNum >= 995)
                    params.add(new BasicBoolean(true));
                conn.run("activeClosePublishConnection", params);
                log.info("Successfully closed publish connection");
            } catch (IOException ioex) {
                throw ioex;
            } finally {
                conn.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            log.error("Unable to actively close the publish connection from site " + site.host + ":" + site.port);
        }

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public AbstractClient() throws SocketException {
        this(DEFAULT_PORT);
    }

    public AbstractClient(int subscribePort) throws SocketException {
        this.listeningPort = subscribePort;
    }

    public AbstractClient(String subscribeHost, int subscribePort) throws SocketException {
        this.listeningHost = subscribeHost;
        this.listeningPort = subscribePort;
    }

    private void addMessageToCache(IMessage msg) {
        String topicString = msg.getTopic();
        String[] topics = topicString.split(",");
        for (String topic : topics) {
            topic = HATopicToTrueTopic.get(topic);
            List<IMessage> cache = messageCache.get(topic);
            if (cache == null) {
                cache = new ArrayList<>();
                messageCache.put(topic, cache);
            }
            cache.add(msg);
        }
    }

    private void flushToQueue() {
        Set<String> keySet = messageCache.keySet();
        for (String topic : keySet) {
            try {
                BlockingQueue<List<IMessage>> q = queueManager.getQueue(topic);
                if (q != null)
                    q.put(messageCache.get(topic));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        messageCache.clear();
    }

    public synchronized void dispatch(IMessage msg) {
        String topicString = msg.getTopic();
        String[] topics = topicString.split(",");
        for (String topic : topics) {
            topic = HATopicToTrueTopic.get(topic);
            BlockingQueue<List<IMessage>> queue = queueManager.getQueue(topic);
            try {
                if (queue != null)
                    queue.put(Arrays.asList(msg));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void batchDispatch(List<IMessage> messags) {
        for (int i = 0; i < messags.size(); ++i) {
            addMessageToCache(messags.get(i));
        }
        flushToQueue();
    }

    public boolean isRemoteLittleEndian(String host) {
        if (hostEndian.containsKey(host)) {
            return hostEndian.get(host);
        } else
            return false;
    }

    public synchronized boolean isClosed(String topic) {
        topic = HATopicToTrueTopic.get(topic);
        synchronized (trueTopicToSites) {
            Site[] sites = trueTopicToSites.get(topic);
            if (sites == null || sites.length == 0)
                return true;
            else
                return sites[0].closed;
        }
    }

    private String getTopic(String host, int port, String alias, String tableName, String actionName) {
        return String.format("%s:%d:%s/%s/%s", host, port, alias, tableName, actionName);
    }

    private String getTopic(String host, int port, String tableName, String actionName) {
        return String.format("%s:%d:%s/%s", host, port, tableName, actionName);
    }

    protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port,
                                                              String tableName, String actionName, MessageHandler handler,
                                                              long offset, boolean reconnect, Vector filter,  StreamDeserializer deserializer, boolean allowExistTopic)
            throws IOException, RuntimeException {
        return subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, "", "", false);
    }

    protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port,
                                                              String tableName, String actionName, MessageHandler handler,
                                                              long offset, boolean reconnect, Vector filter,  StreamDeserializer deserializer,
                                                              boolean allowExistTopic, String userName, String passWord, boolean msgAsTable)
            throws IOException, RuntimeException {
        return subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, allowExistTopic, userName, passWord, msgAsTable, null);
    }

    protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port,
                                                              String tableName, String actionName, MessageHandler handler,
                                                              long offset, boolean reconnect, Vector filter,  StreamDeserializer deserializer,
                                                              boolean allowExistTopic, String userName, String passWord, boolean msgAsTable, List<String> backupSites) throws IOException, RuntimeException {
        checkServerVersion(host, port);
        Entity re;
        String topic = "";
        List<String> tp = Arrays.asList(host, String.valueOf(port), tableName, actionName);
        List<String> usr = Arrays.asList(userName, passWord);
        users.put(tp, usr);
        DBConnection dbConn;
        if (listeningPort > 0)
            dbConn = new DBConnection();
        else
            dbConn = DBConnection.internalCreateEnableReverseStreamingDBConnection(false, false, false, false, false, SqlStdEnum.DolphinDB);

        if (!userName.equals(""))
            dbConn.connect(host, port, userName, passWord);
        else
            dbConn.connect(host, port);
        // set current site index
        currentSiteIndexMap.put(topic, 0);

        if (deserializer!=null&&!deserializer.isInited())
            deserializer.init(dbConn);
        if (deserializer != null){
            BasicDictionary schema = (BasicDictionary) dbConn.run(tableName + ".schema()");
            deserializer.checkSchema(schema);
        }

        String localIP = this.listeningHost;
        if (localIP.equals(""))
            localIP = dbConn.getLocalAddress().getHostAddress();

        if (!hostEndian.containsKey(host)) {
            hostEndian.put(host, dbConn.getRemoteLittleEndian());
        }

        if (Objects.nonNull(backupSites) && !backupSites.isEmpty()) {
            // prepare backupSites
            Site[] sites = new Site[backupSites.size() + 1];
            for (int i = 0; i < sites.length; i++) {
                String ipport = backupSites.get(i);
                String[] parseIpPort = parseIpPort(ipport);
                String backupIP = parseIpPort[0];
                int backupPort = Integer.parseInt(parseIpPort[1]);
                if (i == 0) {
                    sites[i] = new Site(host, port, tableName, actionName, handler, offset - 1, true, filter, deserializer, allowExistTopic, userName, passWord, msgAsTable);
                } else {
                    sites[i] = new Site(backupIP, backupPort, tableName, actionName, handler, offset - 1, true, filter, deserializer, allowExistTopic, userName, passWord, msgAsTable);
                }

                if (!reconnect){
                    sites[i].closed = true;
                }
                synchronized (tableNameToTrueTopic) {
                    tableNameToTrueTopic.put(backupIP + ":" + backupPort + "/" + tableName + "/" + actionName, topic);
                }
                String HATopic = getTopic(backupIP, backupPort, tableName, actionName);
                synchronized (HATopicToTrueTopic) {
                    HATopicToTrueTopic.put(HATopic, topic);
                }
            }
            if (subInfos_.containsKey(topic)){
                throw new RuntimeException("Subscription with topic " + topic + " exist. ");
            }else {
                subInfos_.put(topic, deserializer);
            }
            synchronized (trueTopicToSites) {
                trueTopicToSites.put(topic, sites);
            }
        } else {
            // origin logic：HASites
            try {
                List<Entity> params = new ArrayList<Entity>();
                params.add(new BasicString(tableName));
                params.add(new BasicString(actionName));
                re = dbConn.run("getSubscriptionTopic", params);
                topic = ((BasicAnyVector) re).getEntity(0).getString();
                params.clear();

                params.add(new BasicString(localIP));
                params.add(new BasicInt(this.listeningPort));
                params.add(new BasicString(tableName));
                params.add(new BasicString(actionName));
                params.add(new BasicLong(offset));
                if (filter != null)
                    params.add(filter);
                else {
                    params.add(new Void());
                }
                if (allowExistTopic) {
                    params.add(new BasicBoolean(allowExistTopic));
                }

                re = dbConn.run("publishTable", params);
                connList.add(dbConn);
                if (re instanceof BasicAnyVector) {
                    BasicStringVector HASiteStrings = (BasicStringVector) (((BasicAnyVector) re).getEntity(1));
                    int HASiteNum = HASiteStrings.rows();
                    Site[] sites = new Site[HASiteNum];
                    for (int i = 0; i < HASiteNum; i++) {
                        String HASite = HASiteStrings.getString(i);
                        String[] HASiteHostAndPort = HASite.split(":");
                        String HASiteHost = HASiteHostAndPort[0];
                        int HASitePort = new Integer(HASiteHostAndPort[1]);
                        String HASiteAlias = HASiteHostAndPort[2];
                        sites[i] = new Site(HASiteHost, HASitePort, tableName, actionName, handler, offset - 1, true, filter, deserializer, allowExistTopic, userName, passWord, msgAsTable);
                        if (!reconnect){
                            sites[i].closed = true;
                        }
                        synchronized (tableNameToTrueTopic) {
                            tableNameToTrueTopic.put(HASiteHost + ":" + HASitePort + "/" + tableName + "/" + actionName, topic);
                        }
                        String HATopic = getTopic(HASiteHost, HASitePort, HASiteAlias, tableName, actionName);
                        synchronized (HATopicToTrueTopic) {
                            HATopicToTrueTopic.put(HATopic, topic);
                        }
                    }
                    if (subInfos_.containsKey(topic)){
                        throw new RuntimeException("Subscription with topic " + topic + " exist. ");
                    }else {
                        subInfos_.put(topic, deserializer);
                    }
                    synchronized (trueTopicToSites) {
                        trueTopicToSites.put(topic, sites);
                    }
                } else {
                    Site[] sites = {new Site(host, port, tableName, actionName, handler, offset - 1, reconnect, filter, deserializer, allowExistTopic, userName, passWord, msgAsTable)};
                    if (!reconnect){
                        sites[0].closed = true;
                    }
                    synchronized (subInfos_){
                        subInfos_.put(topic, deserializer);
                    }
                    synchronized (tableNameToTrueTopic) {
                        tableNameToTrueTopic.put(host + ":" + port + "/" + tableName + "/" + actionName, topic);
                    }
                    synchronized (HATopicToTrueTopic) {
                        HATopicToTrueTopic.put(topic, topic);
                    }
                    synchronized (trueTopicToSites) {
                        trueTopicToSites.put(topic, sites);
                    }
                }
            } catch (Exception ex) {
                throw ex;
            } finally {
                if (listeningPort > 0)
                    dbConn.close();
            }
        }

        BlockingQueue<List<IMessage>> queue = queueManager.addQueue(topic);
        return queue;
    }

    protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port,
                                                              String tableName, String actionName, long offset, boolean reconnect)
            throws IOException, RuntimeException {
        return subscribeInternal(host, port, tableName, actionName, null, offset, reconnect, null, null, false);
    }

    protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port, String tableName, long offset) throws IOException, RuntimeException {
        return subscribeInternal(host, port, tableName, DEFAULT_ACTION_NAME, offset, false);
    }

    protected BlockingQueue<List<IMessage>> subscribeInternal(String host, int port, String tableName, String actionName, long offset) throws IOException, RuntimeException {
        return subscribeInternal(host, port, tableName, actionName, offset, false);
    }

    protected void unsubscribeInternal(String host, int port, String tableName, String actionName) throws IOException {
        synchronized (this) {
            DBConnection dbConn = new DBConnection();
            List<String> tp = Arrays.asList(host, String.valueOf(port), tableName, actionName);
            List<String> usr = users.get(tp);
            String user = usr.get(0);
            String pwd = usr.get(1);

            if (!currentSiteIndexMap.isEmpty()) {
                String topic = tableNameToTrueTopic.get( host + ":" + port + "/" + tableName + "/" + actionName);
                Integer currentSiteIndex = currentSiteIndexMap.get(topic);
                Site[] sites = trueTopicToSites.get(topic);
                host = sites[currentSiteIndex].host;
                port = sites[currentSiteIndex].port;
            }

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

                log.info("Successfully unsubscribed table " + fullTableName);
            } catch (Exception ex) {
                throw ex;
            } finally {
                dbConn.close();
            }
        }
    }

    public void close(){
        if(pThread != null)
            pThread.interrupt();
        isClose_ = true;
    }

    public boolean isClose(){
        return isClose_;
    }

    protected void unsubscribeInternal(String host, int port, String tableName) throws IOException {
        unsubscribeInternal(host, port, tableName, DEFAULT_ACTION_NAME);
    }

    void checkServerVersion(String host, int port) throws IOException {
        DBConnection conn = new DBConnection();
        try {
            conn.connect(host, port);
            String version = conn.run("version()").getString();

            String[] _ = version.split(" ")[0].split("\\.");
            int v0 = Integer.parseInt(_[0]);
            int v1 = Integer.parseInt(_[1]);
            int v2 = Integer.parseInt(_[2]);

            if ((v0 == 2 && v1 == 0 && v2 >= 9) || (v0 == 2 && v1 == 10) || (v0 == 3 && v1 == 0 && v2 >= 0)) {
                //server only support reverse connection
                this.listeningPort = 0;
            } else {
                //server Not support reverse connection
                if (this.listeningPort == 0)
                    throw new IOException("The server does not support subscription through reverse connection (connection initiated by the subscriber). Specify a valid port parameter.");
            }
            if(daemon == null) {
                synchronized (connList) {
                    if(daemon == null) {
                        daemon = new Daemon(this.listeningPort, this, connList);
                        pThread = new Thread(daemon);
                        daemon.setRunningThread(pThread);
                        pThread.start();
                    }
                }
            }
        } finally {
            conn.close();
        }
    }

    public ConcurrentHashMap<String, Site[]> getTopicToSites() {
        return trueTopicToSites;
    }

    private static String[] parseIpPort(String ipport) {
        String[] res = new String[2];
        String[] v = ipport.split(":");
        if (v.length < 2)
            throw new RuntimeException("The format of backupSite " + ipport + " is incorrect, should be host:port, e.g. 192.168.1.1:8848");

        res[0] = v[0];
        res[1] = v[1];
        if (Integer.parseInt(res[1]) <= 0 || Integer.parseInt(res[1]) > 65535)
            throw new RuntimeException("The format of backupSite " + ipport + " is incorrect, port should be a positive integer less or equal to 65535");

        return res;
    }
}
