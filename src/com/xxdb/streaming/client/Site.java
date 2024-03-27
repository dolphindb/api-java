package com.xxdb.streaming.client;

import com.xxdb.data.Vector;

public class Site {
    String host;
    int port;
    String tableName;
    String actionName;
    MessageHandler handler;
    long msgId;
    boolean reconnect;
    Vector filter = null;
    boolean closed = false;
    boolean allowExistTopic = false;
    StreamDeserializer deserializer;
    String userName = "";
    String passWord = "";

    boolean msgAstable = false;

    Site(String host, int port, String tableName, String actionName,
         MessageHandler handler, long msgId, boolean reconnect, Vector filter, StreamDeserializer deserializer, boolean allowExistTopic, String userName, String passWord, boolean msgAstable) {
        this.host = host;
        this.port = port;
        this.tableName = tableName;
        this.actionName = actionName;
        this.handler = handler;
        this.msgId = msgId;
        this.reconnect = reconnect;
        this.filter = filter;
        this.allowExistTopic = allowExistTopic;
        this.deserializer = deserializer;
        this.userName = userName;
        this.passWord = passWord;
        this.msgAstable = msgAstable;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getTableName() {
        return tableName;
    }

    public String getActionName() {
        return actionName;
    }

    public MessageHandler getHandler() {
        return handler;
    }

    public long getMsgId() {
        return msgId;
    }

    public boolean isReconnect() {
        return reconnect;
    }

    public Vector getFilter() {
        return filter;
    }

    public boolean isClosed() {
        return closed;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    public boolean isAllowExistTopic() {
        return allowExistTopic;
    }

    public StreamDeserializer getDeserializer() {
        return deserializer;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassWord() {
        return passWord;
    }

    public boolean isMsgAstable() {
        return msgAstable;
    }
}
