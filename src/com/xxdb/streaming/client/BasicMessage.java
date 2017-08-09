package com.xxdb.streaming.client;

import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.Entity;

public class BasicMessage implements IMessage {
	private long offset = 0;
	private String topic = "";
	private BasicAnyVector msg = null;
	
	public BasicMessage(long offset,String topic, BasicAnyVector msg){
		this.offset = offset;
		this.topic = topic;
		this.msg = msg;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> T getValue(int colIndex) {
		return (T)this.msg.getEntity(colIndex);
	}

	@Override
	public String getTopic() {
		return this.topic;
	}

	@Override
	public long getOffset() {
		return this.offset;
	}

	@Override
	public Entity getEntity(int colIndex) {
		return this.msg.getEntity(colIndex);
	}

}
