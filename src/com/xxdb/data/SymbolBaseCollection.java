package com.xxdb.data;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class SymbolBaseCollection {
	private Map<Integer, SymbolBase> symbaseMap = new HashMap<Integer, SymbolBase>();
	private Map<SymbolBase, Integer> existingBases;
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
	
	public void write(ExtendedDataOutput out, SymbolBase base) throws IOException{
		boolean existing = false;
		int id = 0;
		if(existingBases == null){
			existingBases = new HashMap<SymbolBase, Integer>();
			existingBases.put(base, 0);
		}
		else {
			Integer curId = existingBases.get(base);
			if(curId != null){
				existing = true;
				id = curId;
			}
			else {
				id = existingBases.size();
				existingBases.put(base, id);
			}
		}
		out.writeInt(id);
		if(existing){
			out.writeInt(0);
		}
		else{
			int size = base.size();
			out.writeInt(size);
			for(int i=0; i<size; ++i)
				out.writeString(base.getSymbol(i));
		}
	}
	
	public SymbolBase getLastSymbolBase() {
		return lastSymbase;
	}
	
	public void clear(){
		symbaseMap.clear();
		lastSymbase = null;
	}
}
