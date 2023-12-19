package com.xxdb.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class SymbolBase {
	private ArrayList<String> syms = new ArrayList<String>();
	private Map<String, Integer> symMap = null;
	private int id;
	
	
	public SymbolBase(int id) {
		this.id = id;
	}
	
	public SymbolBase(ExtendedDataInput in) throws IOException{
		this(in.readInt(), in);
	}
	
	public SymbolBase(int id, ExtendedDataInput in) throws IOException{
		this.id = id;
		int size = in.readInt();
		for(int i=0; i<size; ++i)
			syms.add(in.readString());
	}
	
	public int getId() {
		return id;
	}
	
	public int size(){
		return syms.size();
	}
	
	public String getSymbol(int index){
		return syms.get(index);
	}
	
	public int find(String key) {
		if(symMap == null){
			symMap = new HashMap<String, Integer>();
			if(syms.size() > 0 && !syms.get(0).isEmpty()) 
				throw new RuntimeException("A symbol base's first key must be empty string.");
			if(syms.size() == 0){
				symMap.put("", 0);
				syms.add("");
			}
			else{
				int count = syms.size();
				for(int i=0; i<count; ++i)
					symMap.put(syms.get(i), i);
			}
		}
		return symMap.getOrDefault(key, -1);
	}
	
	public int find(String key, boolean insertIfNotPresent){
		if(key == null)
			throw new RuntimeException("A symbol base key string can't be null.");
		if(symMap == null){
			symMap = new HashMap<String, Integer>();
			if(syms.size() > 0 && !syms.get(0).isEmpty()) 
				throw new RuntimeException("A symbol base's first key must be empty string.");
			if(syms.size() == 0){
				symMap.put("", 0);
				syms.add("");
			}
			else{
				int count = syms.size();
				for(int i=0; i<count; ++i)
					symMap.put(syms.get(i), i);
			}
		}
		Integer index = symMap.get(key);
		if(index == null){
			index = symMap.size();
			symMap.put(key, index);
			syms.add(key);
		}
		return index;
	}
	
	public void write(ExtendedDataOutput out) throws IOException{
		int count = syms.size();
		out.writeInt(0);
		out.writeInt(count);
		for(int i=0; i<count; ++i)
			out.writeString(syms.get(i), true);
	}
}
