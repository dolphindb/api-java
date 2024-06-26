package com.xxdb.streaming.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class TopicManager {

    private HashMap<String, TopicInfo> topicQueue = new HashMap<>();
    //singleton
    private static TopicManager uniqueInstance = null;

    private TopicManager() {
    }

    public static TopicManager getInstance() {
        if (uniqueInstance == null) {
            uniqueInstance = new TopicManager();
        }
        return uniqueInstance;
    }

    private TopicInfo getTopicInfo(String topic) {
        if (topicQueue.containsKey(topic))
            return topicQueue.get(topic);
        else
            return null;
    }

    public boolean isTopicExists(String topic) {
        return topicQueue.containsKey(topic);
    }

    public synchronized void addTopic(String topic) {
        if (!topicQueue.containsKey(topic)) {
            topicQueue.put(topic, new TopicInfo(topic));
        }
    }

    public synchronized void removeTopic(String topic) {
        if (topicQueue.containsKey(topic)) {
            topicQueue.remove(topic);
        }
    }

    public ConcurrentHashMap<String, Integer> getNameToIndex(String topic) {
        TopicInfo ti = getTopicInfo(topic);
        if (ti != null) {
            return ti.nameToIndex;
        }
        return null;
    }

    public BlockingQueue<List<IMessage>> getMessageQueue(String topic) {
        TopicInfo ti = getTopicInfo(topic);
        BlockingQueue<List<IMessage>> q = ti.messageQueue;
        return q;
    }

    public synchronized BlockingQueue<List<IMessage>> addMessageQueue(String topic) {
        TopicInfo ti = getTopicInfo(topic);
        if (ti != null) {
            if (ti.messageQueue == null) {
                ti.messageQueue = new ArrayBlockingQueue<>(4096);
                return ti.messageQueue;
            }
        }
        return null;
    }

    public List<String> getAllTopic() {
        java.util.Iterator<String> its = topicQueue.keySet().iterator();
        List<String> re = new ArrayList<>();
        while (its.hasNext()) {
            re.add(its.next());
        }
        return re;
    }

    public Site[] getSites(String topic) {
        TopicInfo ti = getTopicInfo(topic);
        Site[] q = ti.sites;
        return q;
    }

    public String getTopic(String HATopic) {
        TopicInfo ti = getTopicInfo(HATopic);
        return ti.originTopic;
    }

    public void setSites(String topic, Site[] sites) {
        TopicInfo ti = getTopicInfo(topic);
        ti.sites = sites;
    }


    private class TopicInfo {
        private String originTopic; // originTopic is same to
        private BlockingQueue<List<IMessage>> messageQueue;
        private ConcurrentHashMap<String, Integer> nameToIndex;
        private Site[] sites;
        private int reconnectStat;

        public TopicInfo(String topic) {
            this.originTopic = topic;
            this.reconnectStat = 0;
        }

        public void setOriginTopic(String originTopic) {
            this.originTopic = originTopic;
        }

        public String getOriginTopic() {
            return this.originTopic;
        }

        public void setNameToIndex(ConcurrentHashMap<String, Integer> nameToIndex) {
            this.nameToIndex = nameToIndex;
        }

        public ConcurrentHashMap<String, Integer> getNameToIndex() {
            return this.nameToIndex;
        }

        public void setReconnectStat(int stat) {
            this.reconnectStat = stat;
        }

        public int getReconnectStat() {
            return this.reconnectStat;
        }

        public void setSites(Site[] sites) {
            this.sites = sites;
        }

        public Site[] getSites() {
            return this.sites;
        }
    }

    public static class Utils {
        public static String getSiteFromTopic(String topic) {
            String site = topic.substring(0, topic.indexOf("/"));
            return site;
        }
    }
}



