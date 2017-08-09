package com.xxdb.streaming.client;

import com.xxdb.streaming.client.IMessage;

import java.util.List;

interface MessageDispatcher {
	boolean isRemoteLittleEndian(String host);
    void dispatch(IMessage message);
    void batchDispatch(List<IMessage> message);
}
