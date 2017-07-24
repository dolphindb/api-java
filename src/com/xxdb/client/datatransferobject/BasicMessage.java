package com.xxdb.client.datatransferobject;

import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.Entity;

public class BasicMessage implements IMessage {
	
	long _offset = 0;
	
	String _topic = "";
	
	BasicAnyVector _msg = null;
	
	public BasicMessage(long offset,String topic, BasicAnyVector msg){
		this._offset = offset;
		this._topic = topic;
		this._msg = msg;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> T getValue(int colIndex) {
		// TODO Auto-generated method stub
		return (T)this._msg.getEntity(colIndex);
	}

	@Override
	public String getTopic() {
		// TODO Auto-generated method stub
		return this._topic;
	}

	@Override
	public long getOffset() {
		// TODO Auto-generated method stub
		return this._offset;
	}

	@Override
	public Entity getEntity(int colIndex) {
		// TODO Auto-generated method stub
		return this._msg.getEntity(colIndex);
	}

}
