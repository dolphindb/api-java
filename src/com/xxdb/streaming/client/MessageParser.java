package com.xxdb.streaming.client;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import com.xxdb.data.AbstractVector;
import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.BasicEntityFactory;
import com.xxdb.data.Entity;
import com.xxdb.data.EntityFactory;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.LittleEndianDataInputStream;
import com.xxdb.streaming.client.datatransferobject.BasicMessage;
import com.xxdb.streaming.client.datatransferobject.IMessage;

class MessageParser implements Runnable{
		
	private final int MAX_FORM_VALUE = Entity.DATA_FORM.values().length -1;
	private final int MAX_TYPE_VALUE = Entity.DATA_TYPE.values().length -1;

	BufferedInputStream bis = null;
	Socket socket = null;
	MessageDispatcher dispatcher;
	public MessageParser(Socket socket, MessageDispatcher dispatcher){
		this.socket = socket;
		this.dispatcher = dispatcher;
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

			EntityFactory factory = new BasicEntityFactory();
			int form = flag>>8;
			int type = flag & 0xff;
			
			if(form < 0 || form > MAX_FORM_VALUE)
				throw new IOException("Invalid form value: " + form);
			if(type <0 || type > MAX_TYPE_VALUE){
				throw new IOException("Invalid type value: " + type);
				
			}
			Entity.DATA_FORM df = Entity.DATA_FORM.values()[form];
			Entity.DATA_TYPE dt = Entity.DATA_TYPE.values()[type];
			Entity body;
			try
			{
				body =  factory.createEntity(df, dt, in);
			}
			catch(Exception exception) {
				throw exception;
			}
			if(body.isVector()){
				BasicAnyVector dTable = (BasicAnyVector)body;
				
				int colSize = dTable.rows();
				int rowSize = dTable.getEntity(0).rows();
				
				if(rowSize>=1){
					if(rowSize==1){
						BasicMessage rec = new BasicMessage(msgid,topic,dTable);
						dispatcher.dispatch(rec);
					} else {
						List<IMessage> messages = new ArrayList<>(rowSize);
						for(int i=0;i<rowSize;i++){
							BasicAnyVector row = new BasicAnyVector(colSize);
						
							for(int j=0;j<colSize;j++){
//								try{
									AbstractVector vector = (AbstractVector)dTable.getEntity(j);
									Entity entity = vector.get(i);
									row.setEntity(j, entity);
//								} catch (ClassCastException e) {
//									e.printStackTrace();
//								}
							}
							BasicMessage rec = new BasicMessage(msgid,topic,row);
							messages.add(rec);
						}
						dispatcher.batchDispatch(messages);
					}
				}
			} else {
				throw new RuntimeException("body is not a vector");
			}
		}
	} catch (Exception e) {
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