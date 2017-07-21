package com.xxdb.consumer;
import java.awt.image.RescaleOp;
import java.io.BufferedInputStream;
import java.io.Console;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.xml.stream.events.EndDocument;

import com.xxdb.consumer.datatransferobject.BasicMessage;
import com.xxdb.consumer.datatransferobject.KafkaMessage;
import com.xxdb.data.AbstractVector;
import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.BasicDateTime;
import com.xxdb.data.BasicDateVector;
import com.xxdb.data.BasicEntityFactory;
import com.xxdb.data.BasicTime;
import com.xxdb.data.BasicTimestamp;
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
	BufferedInputStream bis = null;
	
	SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");//设置日期格式
	public void run(){
		Socket socket = null;
		ServerSocket ssocket = null;
		try {
		ssocket = new ServerSocket(60011);
		System.out.println("acceptint socket...");
		socket  = ssocket.accept();
		System.out.println("socket accepted!");
		if(bis == null) bis= new BufferedInputStream(socket.getInputStream());
		ExtendedDataInput in = new LittleEndianDataInputStream(bis);
		System.out.println("get in stream");

		int ct=0;
		Date ds = new Date();
		while(true){
			
//			Date de = new Date();
//			if(de.getTime()-ds.getTime() >=100){
//				System.out.println("published number/100ms : " + ct);
//				ct = 0;
//				ds = new Date();
//			}
//			System.out.println("begin read");
			
			Boolean b = in.readBoolean(); //true/false : big/Little
			long l = in.readLong();
			String s = in.readString();
			short flag = in.readShort();
//			Timestamp dreceived = new Timestamp(System.currentTimeMillis()); 
			
			EntityFactory factory = new BasicEntityFactory();

			int form = flag>>8;
			int type = flag & 0xff;
			
			if(form < 0 || form > MAX_FORM_VALUE)
				throw new IOException("Invalid form value: " + form);
			if(type <0 || type > MAX_TYPE_VALUE){
				throw new IOException("Invalid type value: " + type);
				
			}
			
			//System.out.println("form" + form + " type :" + type);	
			DATA_FORM df = DATA_FORM.values()[form];
			DATA_TYPE dt = DATA_TYPE.values()[type];
			Entity body  = null;
			try
			{
				body =  factory.createEntity(df, dt, in);
			}
			catch(Exception exception){
//				System.out.println("form" + form + " type :" + type);	
//				System.out.println("short=" + flag);
				continue;
			}

			if(body.isVector()){
				BasicAnyVector dTable = (BasicAnyVector)body;
				
				int colSize = dTable.rows();
				int rowSize = dTable.getEntity(0).rows();
				

//				BasicTimestamp tend = null;
				if(rowSize>=1){
					if(rowSize==1){
						BasicMessage rec = new BasicMessage(l,s,dTable);
						try {
//							if(tend==null) tend = rec.getValue(0);
							Consumer.getMessageQueue().put(rec);
							ct ++;
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
							BasicMessage rec = new BasicMessage(l,s,row);
//							if(tend==null) tend = rec.getValue(0);
							try {
								Consumer.getMessageQueue().put(rec);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						ct += rowSize;
					}
				}
				
//				Timestamp dpub = Timestamp.valueOf(tend.getTimestamp());
//				TimingLogger.AddLog(dpub.getTime(), dreceived.getTime(),rowSize);
//				System.out.println("parsing finished");
//				System.out.println(df1.format(new Date()));
			} else {
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