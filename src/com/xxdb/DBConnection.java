package com.xxdb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.xxdb.data.BasicEntityFactory;
import com.xxdb.data.Entity;
import com.xxdb.data.Entity.DATA_FORM;
import com.xxdb.data.Entity.DATA_TYPE;
import com.xxdb.data.EntityFactory;
import com.xxdb.data.Void;
import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.BigEndianDataOutputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.LittleEndianDataInputStream;
import com.xxdb.io.LittleEndianDataOutputStream;

public class DBConnection {
	private static final int MAX_FORM_VALUE = DATA_FORM.values().length -1;
	private static final int MAX_TYPE_VALUE = DATA_TYPE.values().length -1;
	
	private String sessionID;
	private Socket socket;
	private boolean remoteLittleEndian;
	private ExtendedDataOutput out;
	private EntityFactory factory;
	private String hostName;
	private int port;
	
	public DBConnection(){
		factory = new BasicEntityFactory();
		sessionID = "";
	}
	
	public boolean connect(String hostName, int port) throws IOException{
		this.hostName = hostName;
		this.port = port;
		socket = new Socket(hostName, port);
		out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
		@SuppressWarnings("resource")
		ExtendedDataInput in = new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
	    String body = "connect\n";
		out.writeBytes("API 0 ");
		out.writeBytes(String.valueOf(body.length()));
		out.writeByte('\n');
		out.writeBytes(body);
		out.flush();
		

		String line = in.readLine();
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
		
		return true;
	}
	
	public Entity run(String script) throws IOException{
		boolean reconnect = false;
		if(socket == null || !socket.isConnected() || socket.isClosed() || sessionID.isEmpty())
				throw new IOException("Database connection is not established yet.");

		String body = "script\n"+script;
		
		try{
			out.writeBytes("API "+sessionID+" ");
			out.writeBytes(String.valueOf(body.length()));
			out.writeByte('\n');
			out.writeBytes(body);
			out.flush();
		}
		catch(IOException ex) {
			socket = new Socket(hostName, port);
			out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
			out.writeBytes("API "+sessionID+" ");
			out.writeBytes(String.valueOf(body.length()));
			out.writeByte('\n');
			out.writeBytes(body);
			out.flush();
			reconnect = true;
		}
		catch(Exception ex){
			socket = null;
			throw ex;
		}
		
		@SuppressWarnings("resource")
		ExtendedDataInput in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
			new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
		
		String header = in.readLine();
		String[] headers = header.split(" ");
		if(headers.length != 3)
			throw new IOException("Received invalid header: " + header);
		
		if(reconnect)
			sessionID = headers[0];
		int numObject = Integer.parseInt(headers[1]);
		
		String msg = in.readLine();
		if(!msg.equals("OK"))
			throw new IOException(msg);
		
		if(numObject == 0)
			return new Void();
		
		short flag = in.readShort();
		int form = flag>>8;
		int type = flag & 0xff;
		
		if(form < 0 || form > MAX_FORM_VALUE)
			throw new IOException("Invalid form value: " + form);
		if(type <0 || type > MAX_TYPE_VALUE)
			throw new IOException("Invalid type value: " + type);
		
		DATA_FORM df = DATA_FORM.values()[form];
		DATA_TYPE dt = DATA_TYPE.values()[type];
		
		return factory.createEntity(df, dt, in);
	}
	
	public Entity run(String function, List<Entity> arguments) throws IOException{
		boolean reconnect = false;
		if(socket == null || !socket.isConnected() || socket.isClosed() || sessionID.isEmpty())
			throw new IOException("Database connection is not established yet.");
		
	    String body = "function\n"+function;
		body += ("\n"+ arguments.size() +"\n");
		body += remoteLittleEndian ? "1" : "0";
			
		try{
			out.writeBytes("API "+sessionID+" ");
			out.writeBytes(String.valueOf(body.length()));
			out.writeByte('\n');
			out.writeBytes(body);
			for(int i=0; i<arguments.size(); ++i)
				arguments.get(i).write(out);
			out.flush();
		}
		catch(IOException ex) {
			socket = new Socket(hostName, port);
			out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
			out.writeBytes("API "+sessionID+" ");
			out.writeBytes(String.valueOf(body.length()));
			out.writeByte('\n');
			out.writeBytes(body);
			for(int i=0; i<arguments.size(); ++i)
				arguments.get(i).write(out);
			out.flush();
			reconnect = true;
		}
		catch(Exception ex){
			socket = null;
			throw ex;
		}
		
		@SuppressWarnings("resource")
		ExtendedDataInput in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
			new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
		
		String[] headers = in.readLine().split(" ");
		if(headers.length != 3)
			throw new IOException("Received invalid header.");
		
		if(reconnect)
			sessionID = headers[0];
		int numObject = Integer.parseInt(headers[1]);
		
		String msg = in.readLine();
		if(!msg.equals("OK"))
			throw new IOException(msg);
		
		if(numObject == 0)
			return new Void();
		
		short flag = in.readShort();
		int form = flag>>8;
		int type = flag & 0xff;
		
		if(form < 0 || form > MAX_FORM_VALUE)
			throw new IOException("Invalid form value: " + form);
		if(type <0 || type > MAX_TYPE_VALUE)
			throw new IOException("Invalid type value: " + type);
		
		DATA_FORM df = DATA_FORM.values()[form];
		DATA_TYPE dt = DATA_TYPE.values()[type];
		
		return factory.createEntity(df, dt, in);
	}
	
	public void upload(final Map<String, Entity> variableObjectMap) throws IOException{
		boolean reconnect = false;
		if(socket == null || !socket.isConnected() || socket.isClosed() || sessionID.isEmpty())
			throw new IOException("Database connection is not established yet.");
		
		if(variableObjectMap == null || variableObjectMap.isEmpty())
			return;

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
		catch(Exception ex){
			socket = null;
			throw ex;
		}
		
		ExtendedDataInput in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
			new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
		
		String[] headers = in.readLine().split(" ");
		if(headers.length != 3)
			throw new IOException("Received invalid header.");
		if(reconnect)
			sessionID = headers[0];
		String msg = in.readLine();
		if(!msg.equals("OK"))
			throw new IOException(msg);
	}
	
	public void close(){
		try{
			if(socket != null){
				socket.close();
				socket = null;
			}
		}
		catch(Exception ex){
			ex.printStackTrace();
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
}
