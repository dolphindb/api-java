package com.xxdb.streaming.client;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.xxdb.streaming.client.datatransferobject.IMessage;

class QueueManager {
	private HashMap<String, BlockingQueue<List<IMessage>>> queueMap = new HashMap();
	public synchronized BlockingQueue<List<IMessage>> addQueue(String topic) {
		if(!queueMap.containsKey(topic)){
			BlockingQueue<List<IMessage>> q = new ArrayBlockingQueue<>(4096);
			queueMap.put(topic, q);
			return q;
		}
		throw new RuntimeException("Topic " + topic + " already subscribed");
	}

	public synchronized BlockingQueue<List<IMessage>> getQueue(String topic) {
		BlockingQueue<List<IMessage>> q = queueMap.get(topic);;
		return q;
	}
}