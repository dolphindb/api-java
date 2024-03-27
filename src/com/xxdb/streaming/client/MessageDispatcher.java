package com.xxdb.streaming.client;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

interface MessageDispatcher {
    boolean isRemoteLittleEndian(String host);

    boolean isClosed(String topic);

    boolean isClose();

    void dispatch(IMessage message);

    void batchDispatch(List<IMessage> message);

    boolean tryReconnect(String topic);

    void setMsgId(String topic, long msgId);

    void setNeedReconnect(String topic, int v);

    int getNeedReconnect(String site);

    long getReconnectTimestamp(String site);

    void setReconnectTimestamp(String site, long v);

    List<String> getAllReconnectSites();

    Site getSiteByName(String site);

    void activeCloseConnection(Site site);

    List<String> getAllTopicsBySite(String site);

    Set<String> getAllReconnectTopic();

    Map<String, StreamDeserializer> getSubInfos();

    ConcurrentHashMap<String, Site[]> getTopicToSites();
}


