package com.xxdb.streaming.client;

import com.xxdb.streaming.client.IMessage;

import java.util.List;
import java.util.Set;

interface MessageDispatcher {
    boolean isRemoteLittleEndian(String host);

    boolean isClosed(String topic);

    void dispatch(IMessage message);

    void batchDispatch(List<IMessage> message);

    boolean tryReconnect(String topic);

    void setMsgId(String topic, long msgId);

    void setNeedReconnect(String topic, int v);

    int getNeedReconnect(String site);

    long getReconnectTimestamp(String site);

    void setReconnectTimestamp(String site, long v);

    List<String> getAllReconnectSites();

    AbstractClient.Site getSiteByName(String site);

    void activeCloseConnection(AbstractClient.Site site);

    List<String> getAllTopicsBySite(String site);

    Set<String> getAllReconnectTopic();
}
