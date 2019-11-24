package com.xxdb.streaming.client;

import com.xxdb.streaming.client.IMessage;

import java.util.List;

interface MessageDispatcher {
	boolean isRemoteLittleEndian(String host);
    boolean isClosed(String topic);
    void dispatch(IMessage message);
    void batchDispatch(List<IMessage> message);
    void tryReconnect(String topic);
    void setMsgId(String topic, long msgId);
    void setNeedReconnect(String topic , int v);
    int getNeedReconnect(String topic);
    long getReconnectTimestamp(String topic);

    List<String> getAllTopics();
}
