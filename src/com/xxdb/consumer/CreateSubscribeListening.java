package com.xxdb.consumer;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.xxdb.consumer.datatransferobject.BasicMessage;
import com.xxdb.consumer.datatransferobject.KafkaMessage;
import com.xxdb.data.AbstractVector;
import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.BasicDateVector;
import com.xxdb.data.BasicEntityFactory;
import com.xxdb.data.Entity;
import com.xxdb.data.Entity.DATA_FORM;
import com.xxdb.data.Entity.DATA_TYPE;
import com.xxdb.data.EntityFactory;
import com.xxdb.data.Scalar;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.LittleEndianDataInputStream;

public class CreateSubscribeListening implements Runnable{
	
	private static final int MAX_FORM_VALUE = DATA_FORM.values().length -1;
	private static final int MAX_TYPE_VALUE = DATA_TYPE.values().length -1;
	

	//1。 start socketServer and listening
	//2. receive message
	//3. add message to queue
	public void run(){
		Socket socket = null;
		ServerSocket ssocket = null;
		try {
		ssocket = new ServerSocket(60011);
		System.out.println("acceptint socket...");
		socket  = ssocket.accept();
		System.out.println("socket accepted!");
		ExtendedDataInput in = new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
		System.out.println("get in stream");

		//byte[] r = new byte[1000]; 
		
		while(true){
			
			System.out.println("begin read");
			
			Boolean b = in.readBoolean(); //true/false : big/Little
			long l = in.readLong();
			String s = in.readString();
			short flag = in.readShort();
			
			EntityFactory factory = new BasicEntityFactory();
			
			int form = flag>>8;
			int type = flag & 0xff;
			
			if(form < 0 || form > MAX_FORM_VALUE)
				throw new IOException("Invalid form value: " + form);
			if(type <0 || type > MAX_TYPE_VALUE)
				throw new IOException("Invalid type value: " + type);
			
			DATA_FORM df = DATA_FORM.values()[form];
			DATA_TYPE dt = DATA_TYPE.values()[type];
			
			Entity body =  factory.createEntity(df, dt, in);

			if(body.isVector()){
				BasicAnyVector dTable = (BasicAnyVector)body;
				
				int colSize = dTable.rows();
				int rowSize = dTable.getEntity(0).rows();
				System.out.print("dTable rows : ");
				System.out.print(rowSize);
				System.out.println();
				
				if(rowSize<1) continue;

					if(rowSize==1){
						BasicMessage rec = new BasicMessage(l,s,dTable);
						try {
							Consumer.getMessageQueue().put(rec);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
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
							BasicMessage rec = new BasicMessage(l,s,row);
							try {
								Consumer.getMessageQueue().put(rec);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
		
				SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");//设置日期格式
				System.out.println(df1.format(new Date()));

			}else{
				System.out.println("body is not vector");
				System.out.println(body);
			}
		}

	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} finally {
		try {
			socket.close();
			ssocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	}
	
}