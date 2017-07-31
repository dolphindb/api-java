package com.xxdb.streaming.client;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import com.xxdb.data.*;
import com.xxdb.io.BigEndianDataInputStream;
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
	private final static char[] hexArray = "0123456789ABCDEF".toCharArray();
	public static String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for ( int j = 0; j < bytes.length; j++ ) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}
	public void run(){
		Socket socket = this.socket;
		try {
		if(bis == null) bis= new BufferedInputStream(socket.getInputStream());
		long offset = 0;
		
		String host = socket.getInetAddress().getHostAddress();
		boolean isRemoteLittleEndian = this.dispatcher.isRemoteLittleEndian(host);

		ExtendedDataInput in = isRemoteLittleEndian ? new LittleEndianDataInputStream(bis) : new BigEndianDataInputStream(bis);
		
		while(true){
			Boolean b = in.readBoolean(); //true/false : big/Little
			assert(b == true);
			long msgid = in.readLong();
			if (msgid != offset)
				assert(offset == msgid);
			String topic = in.readString();
			//if (!topic.equals("rh8904_trades1"))
			//	assert(topic.equals("rh8904_trades1"));
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
						//assert ((BasicInt)rec.getEntity(0)).getInt() == 9;
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
							//assert ((BasicInt)rec.getEntity(0)).getInt() == 9;
							messages.add(rec);
							msgid ++;
						}
						dispatcher.batchDispatch(messages);
					}
				}
				if (rowSize != 1024) {
					System.out.println("row Size " + rowSize);
				}
				offset += rowSize;
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