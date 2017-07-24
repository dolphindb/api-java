package com.xxdb.consumer;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

import com.xxdb.consumer.datatransferobject.BasicMessage;
import com.xxdb.consumer.datatransferobject.IMessage;
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

		//1¡£ start socketServer and listening
		//2. receive message
		//3. add message to queue
		BufferedInputStream bis = null;
		
		Socket _socket = null;
		
		public MessageQueueParser(Socket socket){
			this._socket = socket;
		}
		
		
		public void run(){
			Socket socket = this._socket;
			
			try {

			if(bis == null) bis= new BufferedInputStream(socket.getInputStream());
			
			ExtendedDataInput in = new LittleEndianDataInputStream(bis);

			while(true){
				System.out.println("begin read ");
				Boolean b = in.readBoolean(); //true/false : big/Little
				long msgid = in.readLong();
				String topic = in.readString();
				short flag = in.readShort();

				BlockingQueue<IMessage> queue = QueueManager.getQueue(topic);
				
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
					//continue;
				}
				
				
				if(body.isVector()){
					BasicAnyVector dTable = (BasicAnyVector)body;
					
					int colSize = dTable.rows();
					int rowSize = dTable.getEntity(0).rows();
					

					if(rowSize>=1){
						if(rowSize==1){
							BasicMessage rec = new BasicMessage(msgid,topic,dTable);
							try {
								queue.put(rec);
							
							} catch (InterruptedException e) {
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
									queue.put(rec);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
						}
					}
				} else {
					System.out.println("body is not vector");
					System.out.println(body);
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