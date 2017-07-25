package com.xxdb.client;

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.xxdb.client.datatransferobject.IMessage;

public class QueueManager {
		
		private HashMap<String, BlockingQueue<IMessage>> queueMap = new HashMap();
		
		public synchronized BlockingQueue<IMessage> addQueue(String topic) {
			if(!queueMap.containsKey(topic)){
				BlockingQueue<IMessage> q = new ArrayBlockingQueue<>(4096);
				queueMap.put(topic, q);
				return q;
			}
			throw new RuntimeException("Topic " + topic + " already subscribed");
		}
		
		public synchronized BlockingQueue<IMessage> getQueue(String topic) {
			BlockingQueue<IMessage> q = queueMap.get(topic);;
			return q;
		}
		
	}