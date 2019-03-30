package com.xxdb.streaming.client;

import com.xxdb.streaming.client.IMessage;

import java.util.List;

interface MessageDispatcher {
	boolean isRemoteLittleEndian(String host);
    boolean isClosed();
    void dispatch(IMessage message);
    void batchDispatch(List<IMessage> message);
    void tryReconnect(String topic);
    void setMsgId(String topic, long msgId);
}
