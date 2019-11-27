package com.xxdb.streaming.client;

import org.omg.CORBA.INTERNAL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TopicManager{
    private HashMap<String, TopicInfo> topicQueue = new HashMap<>();

    //singleton
    private static TopicManager uniqueInstance = null;
    private TopicManager(){ }

    public static TopicManager getInstance(){
        if(uniqueInstance == null){
            uniqueInstance = new TopicManager();
        }
        return uniqueInstance;
    }

    private TopicInfo getTopicInfo(String topic){
        if(topicQueue.containsKey(topic))
            return topicQueue.get(topic);
        else
            return null;
    }

    public boolean isTopicExists(String topic){
        return topicQueue.containsKey(topic);
    }

    public synchronized void addTopic(String topic){
        if(!topicQueue.containsKey(topic)){
            topicQueue.put(topic, new TopicInfo(topic));
        }
    }

    public synchronized void removeTopic(String topic){
        if(topicQueue.containsKey(topic)){
            topicQueue.remove(topic);
        }
    }

    public HashMap<String,Integer> getNameToIndex(String topic){
        TopicInfo ti = getTopicInfo(topic);
        if(ti!=null){
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
        if(ti!=null){
            if(ti.messageQueue==null){
                ti.messageQueue = new ArrayBlockingQueue<>(4096);
                return ti.messageQueue;
            }
        }
        return null;
    }

    public List<String> getAllTopic(){
        java.util.Iterator<String> its = topicQueue.keySet().iterator();
        List<String> re = new ArrayList<>();
        while(its.hasNext()){
            re.add(its.next());
        }
        return re;
    }

    public AbstractClient.Site[] getSites(String topic) {
        TopicInfo ti = getTopicInfo(topic);
        AbstractClient.Site[] q = ti.sites;
        return q;
    }

    public String getTopic(String HATopic){
        TopicInfo ti = getTopicInfo(HATopic);
        return ti.originTopic;
    }

    public void setSites(String topic, AbstractClient.Site[] sites){
        TopicInfo ti = getTopicInfo(topic);
        ti.sites = sites;
    }


    private class TopicInfo {
        private String originTopic;
        private List<String> HATopicWithTableName;
        private BlockingQueue<List<IMessage>> messageQueue;
        private HashMap<String,Integer> nameToIndex;
        private AbstractClient.Site[] sites;

        public TopicInfo(String topic){
            this.originTopic = topic;
            this.HATopicWithTableName = new ArrayList<>();
            this.nameToIndex = new HashMap<>();
        }
    }
}



