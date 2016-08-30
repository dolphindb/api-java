package com.xxdb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

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
	
	public DBConnection(){
		factory = new BasicEntityFactory();
	}
	
	public boolean connect(String hostName, int port) throws IOException{
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
		if(socket == null || !socket.isConnected()){
			throw new IOException("Database connection is not established yet.");
		}
		
	    String body = "script\n"+script;
		out.writeBytes("API "+sessionID+" ");
		out.writeBytes(String.valueOf(body.length()));
		out.writeByte('\n');
		out.writeBytes(body);
		out.flush();
		
		@SuppressWarnings("resource")
		ExtendedDataInput in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
			new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
		
		String[] headers = in.readLine().split(" ");
		if(headers.length != 3)
			throw new IOException("Received invalid header.");
		
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
		if(socket == null || !socket.isConnected()){
			throw new IOException("Database connection is not established yet.");
		}
		
	    String body = "function\n"+function;
		body += ("\n"+ arguments.size() +"\n");
		body += remoteLittleEndian ? "1" : "0";
		out.writeBytes("API "+sessionID+" ");
		out.writeBytes(String.valueOf(body.length()));
		out.writeByte('\n');
		out.writeBytes(body);
		for(int i=0; i<arguments.size(); ++i)
			arguments.get(i).write(out);
		out.flush();
		
		@SuppressWarnings("resource")
		ExtendedDataInput in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
			new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
		
		String[] headers = in.readLine().split(" ");
		if(headers.length != 3)
			throw new IOException("Received invalid header.");
		
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
	
	public void upload(List<String> variables, List<Entity> arguments) throws IOException{
		if(socket == null || !socket.isConnected()){
			throw new IOException("Database connection is not established yet.");
		}
		int num = variables.size();
		if(num != arguments.size())
			throw new IllegalArgumentException("The size of variables doesn't match the size of objects");
		if(variables.isEmpty())
			return;
		
	    String body = "variable\n";
	    for(int i=0; i<num; ++i){
	    	if(!isVariableCandidate(variables.get(i)))
	    		throw new IllegalArgumentException("'" + variables.get(i) +"' is not a good variable name.");
	    	body +=variables.get(i);
	    	if(i<num - 1)
	    		body += ",";
	    }
		body += ("\n"+ arguments.size() +"\n");
		body += remoteLittleEndian ? "1" : "0";
		out.writeBytes("API "+sessionID+" ");
		out.writeBytes(String.valueOf(body.length()));
		out.writeByte('\n');
		out.writeBytes(body);
		for(int i=0; i<arguments.size(); ++i)
			arguments.get(i).write(out);
		out.flush();
		
		ExtendedDataInput in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
			new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
		
		String[] headers = in.readLine().split(" ");
		if(headers.length != 3)
			throw new IOException("Received invalid header.");
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
