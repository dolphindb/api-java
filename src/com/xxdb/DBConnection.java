package com.xxdb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import com.xxdb.data.BasicBoolean;
import com.xxdb.data.BasicEntityFactory;
import com.xxdb.data.BasicIntVector;
import com.xxdb.data.BasicString;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import com.xxdb.data.EntityFactory;
import com.xxdb.data.Void;
import com.xxdb.io.AbstractExtendedDataOutputStream;
import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.BigEndianDataOutputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.LittleEndianDataInputStream;
import com.xxdb.io.LittleEndianDataOutputStream;
import com.xxdb.io.ProgressListener;

/**
 * Sets up a connection to DolphinDB server through TCP/IP protocol
 * Executes DolphinDB scripts
 * 
 * Example:
 * 
 * import com.xxdb;
 * DBConnection conn = new DBConnection();
 * boolean success = conn.connect("localhost", 8080);
 * conn.run("sum(1..100)");
 *
 */

public class DBConnection {
	private static final int MAX_FORM_VALUE = Entity.DATA_FORM.values().length -1;
	private static final int MAX_TYPE_VALUE = Entity.DATA_TYPE.values().length -1;
	
	private ReentrantLock mutex;
	private String sessionID;
	private Socket socket;
	private boolean remoteLittleEndian;
	private ExtendedDataOutput out;
	private ExtendedDataInput in;
	private EntityFactory factory;
	private String hostName;
	private int port;
	private String userId;
	private String password;
	private String startup = null;
	private boolean encrypted;
	private Site sites[] = null;
	private boolean highAvailability;
	
	class Site {
		private String hostName;
		private int port;

		public Site(String hostName, int port) {
			this.hostName = hostName;
			this.port = port;
		}
		
		public String getHostName() { return hostName; }
		public int getPort() { return port; }
	}

	public DBConnection(){
		factory = new BasicEntityFactory();
		mutex = new ReentrantLock();
		sessionID = "";
	}
	
	public boolean isBusy(){
		if(!mutex.tryLock())
			return true;
		else{
			mutex.unlock();
			return false;
		}
	}
	
	public boolean connect(String hostName, int port) throws IOException{
		return connect(hostName, port, "", "", null, false);
	}

	public boolean connect(String hostName, int port, String startup) throws IOException{
		return connect(hostName, port, "", "", startup, false);
	}
	
	public boolean connect(String hostName, int port, String startup, boolean highAvailability) throws IOException{
		return connect(hostName, port, "", "", startup, highAvailability);
	}

	public boolean connect(String hostName, int port, boolean highAvailability) throws IOException{
		return connect(hostName, port, "", "", null, highAvailability);
	}
	
	public boolean connect(String hostName, int port, String userId, String password) throws IOException{
		return connect(hostName, port, userId, password, null, false);
	}

	public boolean connect(String hostName, int port, String userId, String password, String startup) throws IOException{
		return connect(hostName, port, userId, password, startup, false);
	}
	
	public boolean connect(String hostName, int port, String userId, String password, String startup, boolean highAvailability) throws IOException{
		mutex.lock();
		try{
			if(!sessionID.isEmpty()){
				mutex.unlock();
				return true;
			}
			
			this.hostName = hostName;
			this.port = port;
			this.userId = userId;
			this.password = password;
			this.encrypted = true;
			this.startup = startup;
			this.highAvailability = highAvailability;
			
			return connect();
		}
		finally{
			mutex.unlock();
		}
	}
	
	private boolean connect() throws IOException {
		socket = new Socket(hostName, port);
		socket.setKeepAlive(true);
		socket.setTcpNoDelay(true);
		out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
		@SuppressWarnings("resource")
		ExtendedDataInput input = new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
	    String body = "connect\n";
		out.writeBytes("API 0 ");
		out.writeBytes(String.valueOf(body.length()));
		out.writeByte('\n');
		out.writeBytes(body);
		out.flush();
		

		String line = input.readLine();
		int endPos = line.indexOf(' ');
		if(endPos <= 0){
			close();
			return false;
		}
		sessionID = line.substring(0, endPos);
	
		int startPos = endPos +1;
		endPos = line.indexOf(' ', startPos);
		if(endPos != line.length()-2){
			close();
			return false;
		}
		
		if(line.charAt(endPos +1) == '0'){
			remoteLittleEndian = false;
			out = new BigEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
		}
		else
			remoteLittleEndian = true;
		in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
			new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
		
		if(!userId.isEmpty() && !password.isEmpty())
			login();
		
		if (startup != null && startup.length() > 0)
			run(startup);
		
		if (highAvailability) {
			try {
				BasicTable hostAndPort = (BasicTable) run("select host, port from rpc(getControllerAlias(),getClusterPerf) where mode=0");
				BasicStringVector hosts = (BasicStringVector) hostAndPort.getColumn(0);
				BasicIntVector ports = (BasicIntVector) hostAndPort.getColumn(1);
				int rows = hostAndPort.rows();
				sites = new Site[rows];
				for (int i = 0; i < rows; i++)
					sites[i] = new Site(hosts.getString(i), ports.getInt(i));
			}
			catch (Exception e) {
			}
		}

		return true;
	}
	
	public void login(String userId, String password, boolean enableEncryption) throws IOException{
		mutex.lock();
		try{
			this.userId = userId;
			this.password = password;
			this.encrypted = enableEncryption;
			
			login();
		}
		finally{
			mutex.unlock();
		}
	}
	
	private void login() throws IOException{
		List<Entity> args = new ArrayList<>();
		if(encrypted){
	        BasicString keyCode = (BasicString) run("getDynamicPublicKey()");
			PublicKey key = RSAUtils.getPublicKey(keyCode.getString());
			byte[] usr =  RSAUtils.encryptByPublicKey(userId.getBytes(), key);
		    byte[] pass = RSAUtils.encryptByPublicKey(password.getBytes(), key);
	
	        
	        args.add(new BasicString(Base64.getMimeEncoder().encodeToString(usr)));
	        args.add(new BasicString(Base64.getMimeEncoder().encodeToString(pass)));
	        args.add(new BasicBoolean(true));
		}
		else{
			 args.add(new BasicString(userId));
		     args.add(new BasicString(password));
		}
        run("login", args);
	}
	
	public boolean getRemoteLittleEndian (){
		return this.remoteLittleEndian;
	}
	
	private boolean switchToRandomAvailableSite() throws IOException {
		if (sites.length <= 1)
			return false;
		Site availableSites[] = new Site[sites.length - 1];
		int idx = 0;
		for (int i = 0; i < sites.length; i++) {
			if (sites[i].getHostName() != hostName && sites[i].getPort() != port)
				availableSites[idx++] = sites[i];
		}
		Site nextSite = availableSites[new Random().nextInt(availableSites.length)];
		hostName = nextSite.getHostName();
		port = nextSite.getPort();
		if (startup != null && startup.length() > 0)
			run(startup);
		return true;
	}
	
	public Entity tryRun(String script) throws IOException{
		if(!mutex.tryLock())
			return null;
		try{
			return run(script);
		}
		finally{
			mutex.unlock();
		}
	}
	
	public Entity run(String script) throws IOException{
		return run(script, (ProgressListener)null);
	}
	
	public Entity run(String script, ProgressListener listener) throws IOException{
		mutex.lock();
		try{
			boolean reconnect = false;
			if(socket == null || !socket.isConnected() || socket.isClosed()){
				if(sessionID.isEmpty())
					throw new IOException("Database connection is not established yet.");
				else{
					socket = new Socket(hostName, port);
					out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
				}
			}
	
			String body = "script\n"+script;
			String header = null;
			try{
				out.writeBytes((listener != null ? "API2 " : "API ")+sessionID+" ");
				out.writeBytes(String.valueOf(AbstractExtendedDataOutputStream.getUTFlength(body, 0, 0)));
				out.writeByte('\n');
				out.writeBytes(body);
				out.flush();
				
				header = in.readLine();
			}
			catch(IOException ex) {
				if(reconnect){
					socket = null;
					throw ex;
				}
				
				try {
					connect();
					out.writeBytes((listener != null ? "API2 " : "API ")+sessionID+" ");
					out.writeBytes(String.valueOf(AbstractExtendedDataOutputStream.getUTFlength(body, 0, 0)));
					out.writeByte('\n');
					out.writeBytes(body);
					out.flush();
					
					header = in.readLine();
					reconnect = true;
				}
				catch(Exception e){
					socket = null;
					throw e;
				}
			}

			while(header.equals("MSG")){
				//read intermediate message to indicate the progress
				String msg = in.readString();
				if(listener != null)
					listener.progress(msg);
				header = in.readLine();
			}
			
			String[] headers = header.split(" ");
			if(headers.length != 3){
				socket = null;
				throw new IOException("Received invalid header: " + header);
			}
			
			if(reconnect) {
				sessionID = headers[0];
				if (userId.length() > 0 && password.length() > 0)
					login();
				if (startup != null && startup.length() > 0)
					run(startup);
			}
			int numObject = Integer.parseInt(headers[1]);
			
			String msg = in.readLine();
			if(!msg.equals("OK"))
				throw new IOException(msg);
			
			if(numObject == 0)
				return new Void();
			try{
				short flag = in.readShort();
				int form = flag>>8;
				int type = flag & 0xff;
				
				if(form < 0 || form > MAX_FORM_VALUE)
					throw new IOException("Invalid form value: " + form);
				if(type <0 || type > MAX_TYPE_VALUE)
					throw new IOException("Invalid type value: " + type);
				
				Entity.DATA_FORM df = Entity.DATA_FORM.values()[form];
				Entity.DATA_TYPE dt = Entity.DATA_TYPE.values()[type];
				
				return factory.createEntity(df, dt, in);
			}
			catch(IOException ex){
				socket = null;
				throw ex;
			}
		}
		catch (Exception ex) {
			if (socket != null || !highAvailability || sites == null || sites.length == 0)
				throw ex;
			if (switchToRandomAvailableSite())
				return run(script, listener);
			else
				throw ex;
		}
		finally{
			mutex.unlock();
		}
	}
	
	public Entity tryRun(String function, List<Entity> arguments) throws IOException{
		if(!mutex.tryLock())
			return null;
		try{
			return run(function, arguments);
		}
		finally{
			mutex.unlock();
		}
	}
	
	public Entity run(String function, List<Entity> arguments) throws IOException{
		mutex.lock();
		try{
			boolean reconnect = false;
			if(socket == null || !socket.isConnected() || socket.isClosed()){
				if(sessionID.isEmpty())
					throw new IOException("Database connection is not established yet.");
				else{
					socket = new Socket(hostName, port);
					out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
					in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
						new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
				}
			}
			
		    String body = "function\n"+function;
			body += ("\n"+ arguments.size() +"\n");
			body += remoteLittleEndian ? "1" : "0";
			
			String[] headers = null;
			try{
				out.writeBytes("API "+sessionID+" ");
				out.writeBytes(String.valueOf(body.length()));
				out.writeByte('\n');
				out.writeBytes(body);
				for(int i=0; i<arguments.size(); ++i)
					arguments.get(i).write(out);
				out.flush();
				
				headers = in.readLine().split(" ");
			}
			catch(IOException ex) {
				if(reconnect){
					socket = null;
					throw ex;
				}
				
				try {					
					connect();
					out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
					out.writeBytes("API "+sessionID+" ");
					out.writeBytes(String.valueOf(body.length()));
					out.writeByte('\n');
					out.writeBytes(body);
					for(int i=0; i<arguments.size(); ++i)
						arguments.get(i).write(out);
					out.flush();
					
					in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
						new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
					headers = in.readLine().split(" ");
					reconnect = true;
				}
				catch(Exception e){
					socket = null;
					throw e;
				}
			}

			if(headers.length != 3){
				socket = null;
				throw new IOException("Received invalid header.");
			}
			
			if(reconnect)
				sessionID = headers[0];
			int numObject = Integer.parseInt(headers[1]);
			
			String msg = in.readLine();
			if(!msg.equals("OK"))
				throw new IOException(msg);
			
			if(numObject == 0)
				return new Void();
			
			try{
				short flag = in.readShort();
				int form = flag>>8;
				int type = flag & 0xff;
				
				if(form < 0 || form > MAX_FORM_VALUE)
					throw new IOException("Invalid form value: " + form);
				if(type <0 || type > MAX_TYPE_VALUE)
					throw new IOException("Invalid type value: " + type);
				
				Entity.DATA_FORM df = Entity.DATA_FORM.values()[form];
				Entity.DATA_TYPE dt = Entity.DATA_TYPE.values()[type];
				
				return factory.createEntity(df, dt, in);
			}
			catch(IOException ex){
				socket = null;
				throw ex;
			}
		}
		catch (Exception ex) {
			if (socket != null || !highAvailability || sites == null || sites.length == 0)
				throw ex;
			if (switchToRandomAvailableSite())
				return run(function, arguments);
			else
				throw ex;
		}
		finally{
			mutex.unlock();
		}
	}
	
	public void tryUpload(final Map<String, Entity> variableObjectMap) throws IOException{
		if(!mutex.tryLock())
			throw new IOException("The connection is in use.");
		try{
			upload(variableObjectMap);
		}
		finally{
			mutex.unlock();
		}
	}
	
	public void upload(final Map<String, Entity> variableObjectMap) throws IOException{
		if(variableObjectMap == null || variableObjectMap.isEmpty())
			return;
		
		mutex.lock();
		try{
			boolean reconnect = false;
			if(socket == null || !socket.isConnected() || socket.isClosed()){
				if(sessionID.isEmpty())
					throw new IOException("Database connection is not established yet.");
				else{
					reconnect = true;
					socket = new Socket(hostName, port);
					out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
					in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
						new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
				}
			}
			
			List<Entity> objects = new ArrayList<Entity>();
			
		    String body = "variable\n";
		    for (String key: variableObjectMap.keySet()) {
		    	if(!isVariableCandidate(key))
		    		throw new IllegalArgumentException("'" + key +"' is not a good variable name.");
		    	body += key + ",";
		    	objects.add(variableObjectMap.get(key));
		    }
		    body = body.substring(0, body.length()-1);
			body += ("\n"+ objects.size() +"\n");
			body += remoteLittleEndian ? "1" : "0";
			
			try{
				out.writeBytes("API "+sessionID+" ");
				out.writeBytes(String.valueOf(body.length()));
				out.writeByte('\n');
				out.writeBytes(body);
				for(int i=0; i<objects.size(); ++i)
					objects.get(i).write(out);
				out.flush();
			}
			catch(IOException ex) {
				if(reconnect){
					socket = null;
					throw ex;
				}
				
				try {
					socket = new Socket(hostName, port);
					out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
					out.writeBytes("API "+sessionID+" ");
					out.writeBytes(String.valueOf(body.length()));
					out.writeByte('\n');
					out.writeBytes(body);
					for(int i=0; i<objects.size(); ++i)
						objects.get(i).write(out);
					out.flush();
					reconnect = true;
				}
				catch(Exception e){
					socket = null;
					throw e;
				}
			}
			
			String[] headers = in.readLine().split(" ");
			if(headers.length != 3){
				socket = null;
				throw new IOException("Received invalid header.");
			}
			
			if(reconnect)
				sessionID = headers[0];
			String msg = in.readLine();
			if(!msg.equals("OK"))
				throw new IOException(msg);
		}
		finally{
			mutex.unlock();
		}
	}
	
	public void close(){
		mutex.lock();
		try{
			if(socket != null){
				socket.close();
				sessionID = "";
				socket = null;
			}
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		finally {
			mutex.unlock();
		}
	}
	
	private boolean isVariableCandidate(String word){
		char cur=word.charAt(0);
		if((cur<'a' || cur>'z') && (cur<'A' || cur>'Z'))
			return false;
		for(int i=1;i<word.length();i++){
			cur=word.charAt(i);
			if((cur<'a' || cur>'z') && (cur<'A' || cur>'Z') && (cur<'0' || cur>'9') && cur!='_')
				return false;
		}
		return true;
	}
	
	public String getHostName(){
		return hostName;
	}
	
	public int getPort(){
		return port;
	}
	
	public String getSessionID() {
		return sessionID;
	}
	
	public InetAddress getLocalAddress(){
		return socket.getLocalAddress();	
	}

	public boolean isConnected(){
		return socket != null && socket.isConnected();
	}
}
