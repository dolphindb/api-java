package com.xxdb.streaming.client.streamingSQL;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class StreamingSQLClient extends AbstractClient {

    private DBConnection conn;
    private String host;
    private int port;
    private String userName;
    private String password;
    private BasicIntVector deleteLineMap = new BasicIntVector(0);

    private static final Logger log = LoggerFactory.getLogger(StreamingSQLClient.class);

    // Table wrapper class, used for modifying table references within inner classes
    private static class TableWrapper {
        public BasicTable table;

        public TableWrapper(BasicTable table) {
            this.table = table;
        }
    }

    public StreamingSQLClient(String host, int port, String userName, String password, int subscribePort) throws IOException {
        super(subscribePort);
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        conn = new DBConnection();
        conn.connect(this.host, this.port, this.userName, this.password);
    }

    public void declareStreamingSQLTable(String tableName) {
        try {
            conn.run("declareStreamingSQLTable(" + tableName + ")");
        } catch (IOException e) {
            throw new RuntimeException("declare streaming sql table error: " + e);
        }
    }

    public void revokeStreamingSQLTable(String tableName) {
        try {
            conn.run("revokeStreamingSQLTable(" + tableName + ")");
        } catch (IOException e) {
            throw new RuntimeException("revoke streaming sql table error: " + e);
        }
    }

    public BasicTable listStreamingSQLTables() {
        try {
            return (BasicTable) conn.run("listStreamingSQLTables()");
        } catch (IOException e) {
            throw new RuntimeException("revoke streaming sql table error: " + e);
        }
    }

    public String registerStreamingSQL(String sqlQuery) {
        return registerStreamingSQL(sqlQuery, null, 0);
    }

    public String registerStreamingSQL(String sqlQuery, String queryId) {
        return registerStreamingSQL(sqlQuery, queryId, 0);
    }

    public String registerStreamingSQL(String sqlQuery, int logTableCacheSize) {
        return registerStreamingSQL(sqlQuery, null, logTableCacheSize);
    }

    public String registerStreamingSQL(String sqlQuery, String queryId, int logTableCacheSize) {
        try {
            List<Entity> params = new ArrayList<>();
            params.add(new BasicString(sqlQuery));

            if (Utils.isNotEmpty(queryId)) {
                params.add(new BasicString(queryId));
            }

            if (logTableCacheSize > 0) {
                params.add(new BasicInt(logTableCacheSize));
            }

            Entity streamingSQLTableName = conn.run("registerStreamingSQL", params);
            return streamingSQLTableName.getString();
        } catch (IOException e) {
            throw new RuntimeException("register streaming SQL error: " + e);
        }
    }

    public void revokeStreamingSQL(String queryId) {
        try {
            conn.run("revokeStreamingSQL(" + queryId + ")");
        } catch (IOException e) {
            throw new RuntimeException("revoke streaming SQL error: " + e);
        }
    }

    public BasicTable getStreamingSQLStatus() {
        return getStreamingSQLStatus(null);
    }

    public BasicTable getStreamingSQLStatus(String queryId) {
        try {
            return (BasicTable) conn.run("getStreamingSQLStatus(\"" + queryId + "\")");
        } catch (IOException e) {
            throw new RuntimeException("get streaming SQL status error: " + e);
        }
    }

    public BasicTable subscribeStreamingSQL(String queryId) throws IOException {
        return subscribeStreamingSQL(queryId, -1, -1);
    }

    public BasicTable subscribeStreamingSQL(String queryId, int batchSize, float throttle) throws IOException {
        return subscribeStreamingSQL(queryId, null, -1, -1);
    }

    public BasicTable subscribeStreamingSQL(String queryId, MessageHandler handler) throws IOException {
        return subscribeStreamingSQL(queryId, handler, -1, -1);
    }

    public BasicTable subscribeStreamingSQL(String queryId, MessageHandler handler, int batchSize, float throttle) throws IOException {
        // Create a wrapper to store table references
        final TableWrapper resultWrapper = new TableWrapper(new BasicTable(new ArrayList<>(), new ArrayList<>()));

        if (Objects.isNull(handler)) {
            // update table logic
            handler = new MessageHandler() {
                @Override
                public void doEvent(IMessage msg) {
                    try {
                        log.debug("msg: " + msg.getEntity(0).getString() + " " + msg.getEntity(1).getString() + " " +msg.getEntity(2).getString() + " " +msg.getEntity(3).getString());
                        // Update table and row number mapping
                        StreamingSQLResultUpdater.StreamingSQLResult sqlResult =
                                StreamingSQLResultUpdater.updateStreamingSQLResult(resultWrapper.table, deleteLineMap, msg);

                        // Update table and row number mapping
                        resultWrapper.table = sqlResult.table;
                        deleteLineMap = sqlResult.deleteLineMap;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        // Call subscribeInternal with the (possibly newly created) handler
        Map<String, Object> res = subscribeStreamingSqlLogInfoInternal(host, port, queryId, queryId, handler, -1, false, null, null, false, userName, password, true, true);

        BlockingQueue<List<IMessage>> queue = (BlockingQueue<List<IMessage>>) res.get("queue");
        BasicTable schema = (BasicTable) res.get("schema");
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        for (int i = 2; i < schema.columns(); i++) {
            colNames.add(schema.getColumnName(i));
            cols.add(schema.getColumn(i));
        }

        // Update the table with schema information
        resultWrapper.table = new BasicTable(colNames, cols);

        MessageHandler finalHandler = handler;

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("StreamingSQLClient subscribe start.");
                while (!isClose()) {
                    List<IMessage> msgs = null;
                    if(batchSize == -1 && throttle == -1) {
                        try {
                            msgs = queue.take();
                        } catch (InterruptedException e) {
                            return;
                        }
                    } else if (batchSize != -1 && throttle != -1) {
                        long end;
                        long now = System.currentTimeMillis();
                        end = now + (long)(throttle * 1000);
                        while (msgs == null || (msgs.size()<batchSize && System.currentTimeMillis() < end)) {
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
                    } else {
                        long end;
                        long now = System.currentTimeMillis();
                        end = now + (long)(throttle * 1000);
                        while (msgs == null || System.currentTimeMillis() < end){
                            List<IMessage> tmp = null;
                            try {
                                now = System.currentTimeMillis();
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

                    for (IMessage msg : msgs) {
                        finalHandler.doEvent(msg);
                    }
                }
            }
        });
        thread.start();

        // Returns the table from the wrapper
        return resultWrapper.table;
    }

    public void unsubscribeStreamingSQL(String queryId) {
        try {
            List<Entity> params = new ArrayList<>();
            params.add(new BasicString(queryId));
            conn.run("unsubscribeStreamingSQL", params);
        } catch (IOException e) {
            throw new RuntimeException("revoke streaming SQL error: " + e);
        }
    }

    @Override
    protected boolean doReconnect(Site site) {
        try {
            subscribeStreamingSqlLogInfoInternal(site.getHost(), site.getPort(), site.getTableName(), site.getActionName(), site.getHandler(), site.getMsgId() + 1, true, null, null, false, site.getUserName(), site.getPassWord(), true, true);
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
}