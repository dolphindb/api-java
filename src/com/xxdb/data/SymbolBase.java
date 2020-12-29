package com.xxdb.data;

import java.io.IOException;
import java.util.ArrayList;
import com.xxdb.io.ExtendedDataInput;

public class SymbolBase {
	private ArrayList<String> syms = new ArrayList<String>();
	private int id;
	
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
}
