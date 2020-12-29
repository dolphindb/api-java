package com.xxdb.data;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.xxdb.io.ExtendedDataInput;

public class SymbolBaseCollection {
	private Map<Integer, SymbolBase> symbaseMap = new HashMap<Integer, SymbolBase>();
	private SymbolBase lastSymbase = null;
	
	public SymbolBase add(ExtendedDataInput in) throws IOException{
		int id = in.readInt();
		if(symbaseMap.containsKey(id)){
			int size = in.readInt();
			if(size != 0)
				throw new IOException("Invalid symbol base.");
			lastSymbase = symbaseMap.get(id);
		}
		else{
			SymbolBase cur = new SymbolBase(id, in);
			symbaseMap.put(id, cur);
			lastSymbase = cur;
		}
		return lastSymbase;
	}
	
	public SymbolBase getLastSymbolBase() {
		return lastSymbase;
	}
	
	public void clear(){
		symbaseMap.clear();
		lastSymbase = null;
	}
}
