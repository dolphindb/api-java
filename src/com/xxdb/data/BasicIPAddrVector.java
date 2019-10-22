package com.xxdb.data;

import java.io.IOException;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.Long2;

public class BasicIPAddrVector extends BasicInt128Vector {
	
	public BasicIPAddrVector(int size){
		super(size);
	}
	
	public BasicIPAddrVector(List<Long2> list){
		super(list);
	}
	
	public BasicIPAddrVector(Long2[] array){
		super(array);
	}
	
	protected BasicIPAddrVector(DATA_FORM df, int size){
		super(df, size);
	}
	
	protected BasicIPAddrVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df, in);
	}
	
	public Scalar get(int index){
		return new BasicIPAddr(values[index].high, values[index].low);
	}
	
	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_IPADDR;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicIPAddr.class;
	}
}
