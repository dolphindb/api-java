package com.xxdb.streaming.client;

import com.xxdb.streaming.client.datatransferobject.IMessage;

import java.util.List;

/**
 * Created by root on 7/26/17.
 */
interface MessageDispatcher {
    void dispatch(IMessage message);
    void batchDispatch(List<IMessage> message);
}
