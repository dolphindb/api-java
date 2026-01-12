package com.xxdb.streaming.client.streamingSQL;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.data.Void;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.streaming.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class StreamingSQLClient extends AbstractClient {

    private DBConnection conn;
    private String host;
    private int port;
    private String userName;
    private String password;
    private BasicIntVector deleteLineMap = new BasicIntVector(0);
    private final ConcurrentHashMap<String, SubscriptionInfo> subscriptionInfoCache = new ConcurrentHashMap<>();

    private static final Logger log = LoggerFactory.getLogger(StreamingSQLClient.class);

    private static class SubscriptionInfo {
        public volatile BasicTimestamp leadingRecordInsertTime;
        public volatile BasicTimestamp lastRecordInsertTime;
        public volatile BasicTimestamp lastUpdatedTime;

        public SubscriptionInfo() {
            this.leadingRecordInsertTime = new BasicTimestamp(Long.MIN_VALUE);
            this.lastRecordInsertTime = new BasicTimestamp(Long.MIN_VALUE);
            this.lastUpdatedTime = new BasicTimestamp(Long.MIN_VALUE);
        }
    }

    // Table wrapper class, used for modifying table references within inner classes
    private static class TableWrapper {
        public volatile BasicTable table;

        public TableWrapper(BasicTable table) {
            this.table = table;
        }
    }

    private class ProxyTable extends BasicTable {
        private final TableWrapper wrapper;

        public ProxyTable(TableWrapper wrapper) {
            super(createSafeColumnNames(), createSafeColumns());
            this.wrapper = wrapper;
        }

        @Override
        public int rows() {
            return wrapper.table != null ? wrapper.table.rows() : 0;
        }

        @Override
        public int columns() {
            return wrapper.table != null ? wrapper.table.columns() : 0;
        }

        @Override
        public String getColumnName(int index) {
            return wrapper.table != null ? wrapper.table.getColumnName(index) : "";
        }

        @Override
        public Vector getColumn(int index) {
            return wrapper.table != null ? wrapper.table.getColumn(index) : null;
        }

        @Override
        public Vector getColumn(String name) {
            return wrapper.table != null ? wrapper.table.getColumn(name) : null;
        }

        @Override
        public String getString() {
            return wrapper.table != null ? wrapper.table.getString() : "";
        }

        @Override
        public DATA_CATEGORY getDataCategory() {
            return wrapper.table != null ? wrapper.table.getDataCategory() : DATA_CATEGORY.MIXED;
        }

        @Override
        public DATA_TYPE getDataType() {
            return wrapper.table != null ? wrapper.table.getDataType() : DATA_TYPE.DT_DICTIONARY;
        }

        @Override
        public DATA_FORM getDataForm() {
            return wrapper.table != null ? wrapper.table.getDataForm() : DATA_FORM.DF_TABLE;
        }

        @Override
        public void write(ExtendedDataOutput out) throws IOException {
            if (wrapper.table != null) {
                wrapper.table.write(out);
            }
        }

        @Override
        public Table getSubTable(int[] indices) {
            return wrapper.table != null ? wrapper.table.getSubTable(indices) : null;
        }
    }

    public StreamingSQLClient(String host, int port, String userName, String password) throws IOException {
        if (Utils.isEmpty(host)) {
            throw new IllegalArgumentException("The param 'host' cannot be null or empty.");
        }

        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        conn = new DBConnection();
        conn.connect(this.host, this.port, this.userName, this.password);
    }

    public void declareStreamingSQLTable(String tableName) {
        try {
            checkConnConnect();
            conn.run("declareStreamingSQLTable(" + tableName + ")");
        } catch (IOException e) {
            throw new RuntimeException("declare streaming sql table error: " + e);
        }
    }

    public void revokeStreamingSQLTable(String tableName) {
        try {
            checkConnConnect();
            conn.run("revokeStreamingSQLTable(\"" + tableName + "\")");
        } catch (IOException e) {
            throw new RuntimeException("revoke streaming sql table error: " + e);
        }
    }

    public BasicTable listStreamingSQLTables() {
        try {
            checkConnConnect();
            return (BasicTable) conn.run("listStreamingSQLTables()");
        } catch (IOException e) {
            throw new RuntimeException("revoke streaming sql table error: " + e);
        }
    }

    public String registerStreamingSQL(String sqlQuery) {
        return registerStreamingSQL(sqlQuery, null, Integer.MIN_VALUE);
    }

    public String registerStreamingSQL(String sqlQuery, String queryId) {
        return registerStreamingSQL(sqlQuery, queryId, Integer.MIN_VALUE);
    }

    public String registerStreamingSQL(String sqlQuery, int logTableCacheSize) {
        return registerStreamingSQL(sqlQuery, null, logTableCacheSize);
    }

    public String registerStreamingSQL(String sqlQuery, String queryId, int logTableCacheSize) {
        try {
            if (Utils.isEmpty(sqlQuery)) {
                throw new IllegalArgumentException("The param 'sqlQuery' cannot be null or empty.");
            }

            List<Entity> params = new ArrayList<>();
            params.add(new BasicString(sqlQuery));

            if (Utils.isNotEmpty(queryId)) {
                params.add(new BasicString(queryId));
            } else {
                params.add(new Void());
            }

            if (logTableCacheSize != Integer.MIN_VALUE && logTableCacheSize <= 0) {
                throw new IllegalArgumentException("logTableCacheSize must be a positive integer.");
            } else {
                if (logTableCacheSize == Integer.MIN_VALUE) {
                    params.add(new Void());
                } else {
                    params.add(new BasicInt(logTableCacheSize));
                }
            }

            checkConnConnect();
            Entity streamingSQLTableName = conn.run("registerStreamingSQL", params);
            return streamingSQLTableName.getString();
        } catch (IOException e) {
            throw new RuntimeException("register streaming SQL error: " + e);
        }
    }

    public void revokeStreamingSQL(String queryId) {
        try {
            checkConnConnect();
            conn.run("revokeStreamingSQL(\"" + queryId + "\")");
        } catch (IOException e) {
            throw new RuntimeException("revoke streaming SQL error: " + e);
        }
    }

    public BasicTable getStreamingSQLStatus() {
        return getStreamingSQLStatus(null);
    }

    public BasicTable getStreamingSQLStatus(String queryId) {
        try {
            checkConnConnect();
            if (Objects.equals(queryId, "")) {
                throw new IllegalArgumentException("The param 'queryId' cannot be empty.");
            } else if (Objects.isNull(queryId)) {
                return (BasicTable) conn.run("getStreamingSQLStatus()");
            } else {
                return (BasicTable) conn.run("getStreamingSQLStatus(\"" + queryId + "\")");
            }
        } catch (IOException e) {
            throw new RuntimeException("get streaming SQL status error: " + e);
        }
    }

    public BasicDictionary getStreamingSQLSubscriptionInfo(String queryId) {
        if (Utils.isEmpty(queryId)) {
            throw new IllegalArgumentException("The param 'queryId' cannot be null or empty.");
        }

        // Return data from local cache
        SubscriptionInfo info = subscriptionInfoCache.get(queryId);
        if (info == null) {
            throw new RuntimeException("Subscription info not found for queryId: " + queryId + ". Please subscribe first.");
        }

        // Create BasicDictionary to return
        BasicDictionary result = new BasicDictionary(Entity.DATA_TYPE.DT_STRING, Entity.DATA_TYPE.DT_TIMESTAMP);

        synchronized (info) {
            result.put(new BasicString("leadingRecordInsertTime"), info.leadingRecordInsertTime);
            result.put(new BasicString("lastRecordInsertTime"), info.lastRecordInsertTime);
            result.put(new BasicString("lastUpdatedTime"), info.lastUpdatedTime);
        }

        return result;
    }

    public BasicTable subscribeStreamingSQL(String queryId) throws IOException {
        return subscribeStreamingSQL(queryId, -1, -1, null);
    }

    public BasicTable subscribeStreamingSQL(String queryId, UpdateListener listener) throws IOException {
        return subscribeStreamingSQL(queryId, -1, -1, listener);
    }

    private int countTotalRows(List<IMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return 0;
        }
        int totalRows = 0;
        for (IMessage msg : messages) {
            try {
                BasicTimestampVector timestampVector = (BasicTimestampVector) msg.getEntity(2);
                totalRows += timestampVector.rows();
            } catch (Exception e) {
                log.warn("Failed to get rows from message: " + e.getMessage());
            }
        }
        return totalRows;
    }

    /**
     * Check if the batch is complete by verifying if the last timestamp is non-null
     *
     * @param messages List of messages to check
     * @return true if the last timestamp in the last message is non-null, false otherwise
     */
    private boolean isBatchComplete(List<IMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return false;
        }
        // Get the last message
        IMessage lastMsg = messages.get(messages.size() - 1);
        try {
            BasicTimestampVector timestampVector = (BasicTimestampVector) lastMsg.getEntity(2);
            if (timestampVector != null && timestampVector.rows() > 0) {
                // Check the last timestamp in the vector
                BasicTimestamp lastTimestamp = (BasicTimestamp) timestampVector.get(timestampVector.rows() - 1);
                return lastTimestamp != null && !lastTimestamp.isNull();
            }
        } catch (Exception e) {
            log.warn("Failed to check batch completion: " + e.getMessage());
        }
        return false;
    }

    public BasicTable subscribeStreamingSQL(String queryId, int batchSize, float throttle) throws IOException {
        return subscribeStreamingSQL(queryId, batchSize, throttle, null);
    }

    public BasicTable subscribeStreamingSQL(String queryId, int batchSize, float throttle, UpdateListener listener) throws IOException {
        // Create a wrapper to store table references
        final TableWrapper resultWrapper = new TableWrapper(null);
        final UpdateListener listenerRef = listener;

        // update table logic
        MessageHandler handler = new MessageHandler() {
            @Override
            public void doEvent(IMessage msg) {
                try {
                    // If the table is initialized, update the table content
                    if (resultWrapper.table != null) {
                        // Print the table content before update
                        log.debug("Before update, table content: " + resultWrapper.table.getString());

                        // Get update results
                        StreamingSQLResultUpdater.StreamingSQLResult sqlResult = StreamingSQLResultUpdater.updateStreamingSQLResult(resultWrapper.table, deleteLineMap, msg);
                        // Get the new table and deleteLineMap
                        BasicTable newTable = sqlResult.table;
                        deleteLineMap = sqlResult.deleteLineMap;
                        // Update table references
                        resultWrapper.table = newTable;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        if (Utils.isEmpty(queryId)) {
            throw new IllegalArgumentException("The param 'queryId' cannot be null or empty.");
        }

        subscriptionInfoCache.computeIfAbsent(queryId, key -> new SubscriptionInfo());

        try {
            getStreamingSQLStatus(queryId);
        } catch (Exception e) {
            if (e.getMessage().contains("queryId " + queryId + " does not exist")) {
                throw new IllegalArgumentException("queryId " + queryId + " does not exist.");
            }
        }

        // Call subscribeInternal with the (possibly newly created) handler
        Map<String, Object> res = subscribeStreamingSqlLogInfoInternal(host, port, queryId, queryId, handler, -1, false, null, null, false, userName, password, true, true, true);

        BlockingQueue<List<IMessage>> queue = (BlockingQueue<List<IMessage>>) res.get("queue");

        resultWrapper.table = (BasicTable) res.get("schema");
        log.debug("Created initial table, contain: " + resultWrapper.table.columns() + " cols");

        final ProxyTable proxyTable = new ProxyTable(resultWrapper);

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("StreamingSQLClient subscribe start.");
                try {
                    while (!isClose()) {
                        List<IMessage> msgs = new ArrayList<>();
                        if (batchSize < 0) {
                            while (true) {
                                List<IMessage> tmp;
                                try {
                                    if (isBatchComplete(msgs)) {
                                        tmp = queue.poll(0, TimeUnit.MILLISECONDS);
                                    } else {
                                        tmp = queue.take();
                                    }
                                } catch (InterruptedException e) {
                                    return;
                                }
                                if (tmp != null) {
                                    msgs.addAll(tmp);
                                } else if (isBatchComplete(msgs)) {
                                    break;
                                }
                            }
                        } else if (batchSize > 0 && throttle < 0) {
                            while (true) {
                                List<IMessage> tmp;
                                try {
                                    if (countTotalRows(msgs) >= batchSize && isBatchComplete(msgs)) {
                                        tmp = queue.poll(0, TimeUnit.MILLISECONDS);
                                    } else {
                                        tmp = queue.take();
                                    }
                                } catch (InterruptedException e) {
                                    break;
                                }
                                if (tmp != null) {
                                    msgs.addAll(tmp);
                                } else if (countTotalRows(msgs) >= batchSize && isBatchComplete(msgs)) {
                                    break;
                                }
                            }
                        } else {
                            long end;
                            long now = System.currentTimeMillis();
                            end = now + (long) (throttle * 1000);
                            boolean shouldDrainQueue = false;

                            while (System.currentTimeMillis() < end || shouldDrainQueue) {
                                List<IMessage> tmp;
                                try {
                                    now = System.currentTimeMillis();
                                    if (shouldDrainQueue) {
                                        tmp = queue.poll(0, TimeUnit.MILLISECONDS);
                                    } else if (end - now <= 0) {
                                        tmp = queue.take();
                                    } else {
                                        tmp = queue.poll(end - now, TimeUnit.MILLISECONDS);
                                    }
                                } catch (InterruptedException e) {
                                    break;
                                }

                                if (tmp != null) {
                                    msgs.addAll(tmp);
                                    if (countTotalRows(msgs) >= batchSize && isBatchComplete(msgs)) {
                                        shouldDrainQueue = true;  // Start draining mode
                                    }
                                } else if (shouldDrainQueue || (System.currentTimeMillis() >= end && isBatchComplete(msgs))) {
                                    break;
                                }
                            }
                        }

                        if (msgs.isEmpty())
                            continue;

                        // Extract timestamps from batch messages
                        BasicTimestamp firstNonNullTimestamp = new BasicTimestamp(Long.MIN_VALUE);
                        BasicTimestamp lastNonNullTimestamp = new BasicTimestamp(Long.MIN_VALUE);

                        for (IMessage msg : msgs) {
                            BasicTimestampVector timestampVector = (BasicTimestampVector) msg.getEntity(2);
                            for (int i = 0; i < timestampVector.rows(); i++) {
                                BasicTimestamp basicTimestamp = (BasicTimestamp) timestampVector.get(i);
                                if (basicTimestamp != null && !basicTimestamp.isNull()) {
                                    if (firstNonNullTimestamp.getLong() == Long.MIN_VALUE) {
                                        firstNonNullTimestamp = basicTimestamp;
                                    }
                                    lastNonNullTimestamp = basicTimestamp;
                                }
                            }
                        }

                        for (IMessage msg : msgs) {
                            try {
                                handler.doEvent(msg);
                            } catch (Exception e) {
                                notifyListenerOnError(listenerRef, queryId, e);
                                throw (RuntimeException) e;
                            }
                        }

                        SubscriptionInfo info = subscriptionInfoCache.get(queryId);
                        if (info != null) {
                            synchronized (info) {
                                info.leadingRecordInsertTime = firstNonNullTimestamp;
                                info.lastRecordInsertTime = lastNonNullTimestamp;
                                info.lastUpdatedTime = new BasicTimestamp(LocalDateTime.now());
                            }
                            log.debug("Updated subscription info for queryId=" + queryId +
                                    ", leadingRecordInsertTime=" + info.leadingRecordInsertTime +
                                    ", lastRecordInsertTime=" + info.lastRecordInsertTime +
                                    ", lastUpdatedTime=" + info.lastUpdatedTime);

                            if (listenerRef != null) {
                                List<ChangeRecord> changes = extractChanges(msgs);
                                UpdateEvent event = new UpdateEvent(queryId, proxyTable, changes, new ArrayList<>(msgs), System.currentTimeMillis());
                                notifyListenerOnUpdate(listenerRef, event);
                            }
                        }
                    }
                } finally {
                    notifyListenerOnClose(listenerRef, queryId);
                }
            }
        });
        thread.start();

        // Returns the table from the wrapper
        return proxyTable;
    }

    public void unsubscribeStreamingSQL(String queryId) throws IOException {
        try {
            List<Entity> params = new ArrayList<>();
            params.add(new Void());
            params.add(new BasicString(queryId));
            checkConnConnect();
            conn.run("unsubscribeStreamingSQL", params);

            String topic = null;
            String fullTableName = host + ":" + port + "/" + queryId + "/" + queryId;
            synchronized (tableNameToTrueTopic) {
                topic = tableNameToTrueTopic.get(fullTableName);
            }
            synchronized (trueTopicToSites) {
                Site[] sites = trueTopicToSites.get(topic);
                if (sites == null || sites.length == 0)
                    ;
                for (int i = 0; i < sites.length; i++)
                    sites[i].setClosed(true);
            }
            synchronized (queueManager) {
                queueManager.removeQueue(topic);
            }
            log.info("Successfully unsubscribed table " + fullTableName);
        } catch (IOException e) {
            throw e;
        } finally {
            conn.close();
        }
    }

    @Override
    protected boolean doReconnect(Site site) {
        try {
            subscribeStreamingSqlLogInfoInternal(site.getHost(), site.getPort(), site.getTableName(), site.getActionName(), site.getHandler(), site.getMsgId() + 1, true, null, null, false, site.getUserName(), site.getPassWord(), true, true, true);
            Date d = new Date();
            DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            log.info(df.format(d) + " Successfully reconnected and subscribed " + site.getHost() + ":" + site.getPort() + "/" + site.getTableName() + "/" + site.getActionName());
            return true;
        } catch (Exception ex) {
            Date d = new Date();
            DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            log.error(df.format(d) + " Unable to subscribe table. Will try again after 1 seconds." + site.getHost() + ":" + site.getPort() + "/" + site.getTableName() + "/" + site.getActionName());
            ex.printStackTrace();
            return false;
        }
    }

    private void notifyListenerOnUpdate(UpdateListener listener, UpdateEvent event) {
        if (listener == null || event == null) {
            return;
        }
        try {
            listener.onUpdate(event);
        } catch (Exception ex) {
            log.warn("UpdateListener onUpdate failed: " + ex.getMessage(), ex);
        }
    }

    private void notifyListenerOnError(UpdateListener listener, String queryId, Throwable error) {
        if (listener == null || error == null) {
            return;
        }
        try {
            listener.onError(queryId, error);
        } catch (Exception ex) {
            log.warn("UpdateListener onError failed: " + ex.getMessage(), ex);
        }
    }

    private void notifyListenerOnClose(UpdateListener listener, String queryId) {
        if (listener == null) {
            return;
        }
        try {
            listener.onClose(queryId);
        } catch (Exception ex) {
            log.warn("UpdateListener onClose failed: " + ex.getMessage(), ex);
        }
    }

    private List<ChangeRecord> extractChanges(List<IMessage> msgs) {
        List<ChangeRecord> changes = new ArrayList<>();
        if (msgs == null || msgs.isEmpty()) {
            return changes;
        }
        for (IMessage msg : msgs) {
            try {
                BasicByteVector typeColumn = (BasicByteVector) msg.getEntity(0);
                BasicIntVector lineNoColumn = (BasicIntVector) msg.getEntity(1);
                int rowCount = typeColumn.rows();
                for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                    byte typeByte = ((BasicByte) typeColumn.get(rowIdx)).getByte();
                    int lineNo = ((BasicInt) lineNoColumn.get(rowIdx)).getInt();

                    List<Entity> rowData = new ArrayList<>();
                    for (int colIdx = 3; colIdx < msg.size(); colIdx++) {
                        Entity colEntity = msg.getEntity(colIdx);
                        if (colEntity instanceof Vector) {
                            rowData.add(((Vector) colEntity).get(rowIdx));
                        } else {
                            rowData.add(colEntity);
                        }
                    }

                    ChangeType changeType = ChangeType.toChangeType(typeByte);
                    changes.add(new ChangeRecord(changeType, lineNo, rowData));
                }
            } catch (Exception ex) {
                log.warn("Failed to extract changes from message: " + ex.getMessage(), ex);
            }
        }
        return changes;
    }

    private void checkConnConnect() throws IOException {
        if (!conn.isConnected()) {
            conn = new DBConnection();
            conn.connect(this.host, this.port, this.userName, this.password);
        }
    }

    private static List<String> createSafeColumnNames() {
        List<String> columnNames = new ArrayList<>();
        columnNames.add("dummy");
        return columnNames;
    }

    private static List<Vector> createSafeColumns() {
        List<Vector> columns = new ArrayList<>();
        columns.add(new BasicStringVector(0));
        return columns;
    }
}
