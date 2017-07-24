package com.xxdb.consumer;

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.xxdb.consumer.datatransferobject.IMessage;

public class QueueManager {
		
		private static HashMap<String, BlockingQueue<IMessage>> _queueMap = new HashMap();
		
		public static void addQueue(String topic) {
			if(!_queueMap.containsKey(topic)){
				BlockingQueue<IMessage> q = new ArrayBlockingQueue<>(4096);
				_queueMap.put(topic, q);
			}
		}
		
		public static BlockingQueue<IMessage> getQueue(String topic) {
			BlockingQueue<IMessage> q = null;
			if(_queueMap.containsKey(topic)){
				 q = _queueMap.get(topic);
			}
			return q;
		}
		
	}