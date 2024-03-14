package com.xxdb.streaming.client;

import java.util.HashMap;

import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.Entity;

public class BasicMessage implements IMessage {
    private long offset = 0;
    private String topic = "";
    private BasicAnyVector msg = null;
    private HashMap<String, Integer> nameToIndex = null;
    private String sym_ = "";

    public BasicMessage(long offset, String topic, BasicAnyVector msg, HashMap<String, Integer> nameToIndex) {
        this.offset = offset;
        this.topic = topic;
        this.msg = msg;
        this.nameToIndex = nameToIndex;
    }

    public BasicMessage(long offset, String topic, BasicAnyVector msg, String sym)
    {
        this.offset = offset;
        this.topic = topic;
        this.msg = msg;
        this.sym_ = sym;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getValue(int colIndex) {
        return (T) this.msg.getEntity(colIndex);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getValue(String colName) {
        int colIndex = nameToIndex.get(colName.toLowerCase());
        return (T) this.msg.getEntity(colIndex);
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

    public Entity getMsg() {
        return this.msg;
    }

    public int size(){
        return msg.rows();
    }

    public String getSym(){
        return sym_;
    }
}
