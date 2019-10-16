package com.xxdb.data;

import java.io.IOException;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.Long2;

public class BasicUuidVector extends BasicInt128Vector {
	
	public BasicUuidVector(int size){
		super(size);
	}
	
	public BasicUuidVector(List<Long2> list){
		super(list);
	}
	
	public BasicUuidVector(Long2[] array){
		super(array);
	}
	
	protected BasicUuidVector(DATA_FORM df, int size){
		super(df, size);
	}
	
	protected BasicUuidVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df, in);
	}
	
	public Scalar get(int index){
		return new BasicUuid(values[index].high, values[index].low);
	}
	
	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_UUID;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicUuid.class;
	}
}
