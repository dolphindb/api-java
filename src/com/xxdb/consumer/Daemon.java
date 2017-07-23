package com.xxdb.consumer;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.IllegalFormatCodePointException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.omg.CORBA.PolicyListHelper;

import com.xxdb.DBConnection;
import com.xxdb.consumer.datatransferobject.BasicMessage;
import com.xxdb.consumer.datatransferobject.IMessage;
import com.xxdb.data.AbstractVector;
import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.BasicEntityFactory;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicString;
import com.xxdb.data.Entity;
import com.xxdb.data.EntityFactory;
import com.xxdb.data.Entity.DATA_FORM;
import com.xxdb.data.Entity.DATA_TYPE;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.LittleEndianDataInputStream;

public class Daemon {
	
	private ConsumerListenerManager _lsnMgr = null;
	private String _host = "";
	private int _port = 0;

	private int _workerNumber = 0;
	
	ServerSocket ssocket = null;
	
	private int _listeningPort = 60011;
	
	public void setSubscriptPort(int port){
		this._listeningPort = port;
	}
	
	public Daemon(String host,int port) {
		this._lsnMgr = new ConsumerListenerManager();
		this._host = host;
		this._port = port;
		try {
			ssocket = new ServerSocket(this._listeningPort);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void setConsumeThreadNumber (int number){
		this._workerNumber = number;
	}
	
	protected static BlockingQueue<IMessage> getMessageQueue(String topic){
		return QueueManager.getQueue(topic);
	}
	
	protected ConsumerListenerManager getConsumerListenerManager() {
		return this._lsnMgr;
	}
	
	private String handleSubscribe(String tableName,MessageIncomingHandler handler) {

		Entity re;
		DBConnection dbConn = new DBConnection();
		String topic = "";
		try {
			dbConn.connect(this._host, this._port);

			List<Entity> params = new ArrayList<Entity>();

			params.add(new BasicString(tableName));
			re = dbConn.run("getSubscriptionTopic", params);
			topic = re.getString();
			System.out.println("getSubscriptionTopic:" + topic);

			//start thread for socket accept when got a topic
			CreateSubscribeListening subscribeClient = new CreateSubscribeListening(topic,this.ssocket);
			Thread listeningThread = new Thread(subscribeClient);
			listeningThread.start();
			if(handler!=null)
				this._lsnMgr.addMessageIncomingListener(topic, handler);
			QueueManager.addQueue(topic);
			params.clear();

			params.add(new BasicString("localhost"));
			params.add(new BasicInt(this._listeningPort));
			params.add(new BasicString(tableName));
			re = dbConn.run("publishTable", params);
			System.out.println("publishTable:" + re.getString());

		} catch (IOException e) {

			e.printStackTrace();
		}	
		
		if(handler!=null){
			if(this._workerNumber<=1){
				MessageQueueWorker consumeQueueWorker = new MessageQueueWorker(topic,this._lsnMgr);
				Thread myThread2 = new Thread(consumeQueueWorker);
				myThread2.start();
			} else {
				ExecutorService pool = Executors.newCachedThreadPool();
				for (int i=0;i<this._workerNumber;i++) {
					pool.execute(new MessageQueueWorker(topic,this._lsnMgr));
				}
			}
		}
		
		return topic;
		
	}
	
	public void subscribe(String tableName,MessageIncomingHandler handler){
		handleSubscribe(tableName, handler);
	}
	
	public String subscribe(String tableName){
		return handleSubscribe(tableName,null);
	}
	
	public ArrayList<IMessage> poll(String topic, long timeout){

		ArrayList<IMessage> reArray = new ArrayList<IMessage>();
		BlockingQueue<IMessage> queue = QueueManager.getQueue(topic);
		Date ds = new Date();
		 long start = ds.getTime();
         long remaining = timeout;
         
		do{
			if(queue.isEmpty() == false){
				try {
					reArray.add(queue.take());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			long elapsed = (new Date()).getTime() - start;
            remaining = timeout - elapsed;
		} while (remaining > 0);
		return reArray;
	}
	
	public class CreateSubscribeListening implements Runnable{
		
		private final int MAX_FORM_VALUE = DATA_FORM.values().length -1;
		private final int MAX_TYPE_VALUE = DATA_TYPE.values().length -1;

		//1。 start socketServer and listening
		//2. receive message
		//3. add message to queue
		BufferedInputStream bis = null;
		
		ServerSocket _serverSocket = null;
		
//		private String _topic = "";
//		
		public CreateSubscribeListening(String topic,ServerSocket serverSocket){
			this._serverSocket = serverSocket;
//			this._topic = topic;
		}
		

		
		SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");//设置日期格式
		
		public void run(){
			Socket socket = null;
			
			try {
		
			System.out.println("acceptint socket...");
			socket  = this._serverSocket.accept();
			System.out.println("socket accepted!");
			if(bis == null) bis= new BufferedInputStream(socket.getInputStream());
			ExtendedDataInput in = new LittleEndianDataInputStream(bis);
			System.out.println("get in stream");

			int ct=0;
			Date ds = new Date();
			while(true){
				
//				Date de = new Date();
//				if(de.getTime()-ds.getTime() >=100){
//					System.out.println("published number/100ms : " + ct);
//					ct = 0;
//					ds = new Date();
//				}
//				System.out.println("begin read");
				
				Boolean b = in.readBoolean(); //true/false : big/Little
				long msgid = in.readLong();
				String topic = in.readString();
				short flag = in.readShort();

				BlockingQueue<IMessage> queue = Daemon.getMessageQueue(topic);
				
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
//					System.out.println("form" + form + " type :" + type);	
//					System.out.println("short=" + flag);
					continue;
				}
				
				
				if(body.isVector()){
					BasicAnyVector dTable = (BasicAnyVector)body;
					
					int colSize = dTable.rows();
					int rowSize = dTable.getEntity(0).rows();
					

//					BasicTimestamp tend = null;
					if(rowSize>=1){
						if(rowSize==1){
							BasicMessage rec = new BasicMessage(msgid,topic,dTable);
							try {
//								if(tend==null) tend = rec.getValue(0);
								queue.put(rec);
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
								BasicMessage rec = new BasicMessage(msgid,topic,row);
//								if(tend==null) tend = rec.getValue(0);
								try {
									queue.put(rec);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							ct += rowSize;
						}
					}
					
//					Timestamp dpub = Timestamp.valueOf(tend.getTimestamp());
//					TimingLogger.AddLog(dpub.getTime(), dreceived.getTime(),rowSize);
//					System.out.println("parsing finished");
//					System.out.println(df1.format(new Date()));
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
	
	public static class QueueManager {
		
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
	
	
	
}

