package com.xxdb.client;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import com.xxdb.client.datatransferobject.BasicMessage;
import com.xxdb.client.datatransferobject.IMessage;
import com.xxdb.data.AbstractVector;
import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.BasicEntityFactory;
import com.xxdb.data.Entity;
import com.xxdb.data.EntityFactory;
import com.xxdb.data.Entity.DATA_FORM;
import com.xxdb.data.Entity.DATA_TYPE;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.LittleEndianDataInputStream;

public class MessageQueueParser implements Runnable{
		
	private final int MAX_FORM_VALUE = DATA_FORM.values().length -1;
	private final int MAX_TYPE_VALUE = DATA_TYPE.values().length -1;

	BufferedInputStream bis = null;
	Socket socket = null;
	QueueManager queueManager;
	public MessageQueueParser(Socket socket, QueueManager queueManager){
		this.socket = socket;
		this.queueManager = queueManager;
	}
	
	public void run(){
		Socket socket = this.socket;
		
		try {
		if(bis == null) bis= new BufferedInputStream(socket.getInputStream());
	
		ExtendedDataInput in = new LittleEndianDataInputStream(bis);
		while(true){
			
			Boolean b = in.readBoolean(); //true/false : big/Little
			long msgid = in.readLong();
			String topic = in.readString();
			short flag = in.readShort();
				
			BlockingQueue<IMessage> queue = queueManager.getQueue(topic);
				
			EntityFactory factory = new BasicEntityFactory();
			int form = flag>>8;
			int type = flag & 0xff;
			
			if(form < 0 || form > MAX_FORM_VALUE)
				throw new IOException("Invalid form value: " + form);
			if(type <0 || type > MAX_TYPE_VALUE){
				throw new IOException("Invalid type value: " + type);
				
			}
			DATA_FORM df = DATA_FORM.values()[form];
			DATA_TYPE dt = DATA_TYPE.values()[type];
			Entity body  = null;
			try
			{
				body =  factory.createEntity(df, dt, in);
			}
			catch(Exception exception){
				throw exception;
			}
	           
			if(body.isVector()){
				BasicAnyVector dTable = (BasicAnyVector)body;
				
				int colSize = dTable.rows();
				int rowSize = dTable.getEntity(0).rows();
				
				if(rowSize>=1){
					if(rowSize==1){
						BasicMessage rec = new BasicMessage(msgid,topic,dTable);
						try {
							if (queue.offer(rec) && queue.size() == 1) {
								synchronized (queueManager) {
									queueManager.notify();
								}
							} else {
								while (queue.offer(rec) == false)
									synchronized (queueManager) {
										queueManager.notify();
									}
							}
							
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else {
						for(int i=0;i<rowSize;i++){
							BasicAnyVector row = new BasicAnyVector(colSize);
						
							for(int j=0;j<colSize;j++){
								AbstractVector vector = (AbstractVector)dTable.getEntity(j);
								Entity entity = vector.get(i);
								row.setEntity(j, entity);
							}
							BasicMessage rec = new BasicMessage(msgid,topic,row);
							try {
								if (queue.offer(rec)) {
									if (queue.size() == 1) {
										synchronized (queueManager) {
											queueManager.notify();
										}
									}
								} else {
									while (queue.offer(rec) == false) {
										synchronized (queueManager) {
											queueManager.notify();
										}
									}
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
			} else {
				throw new RuntimeException("body is not a vector");
			}
		}
	} catch (IOException e) {
		e.printStackTrace();
	} finally {
		try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
		
}