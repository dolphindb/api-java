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

	@Override
	public Vector combine(Vector vector) {
		BasicIPAddrVector v = (BasicIPAddrVector)vector;
		int newSize = this.rows() + v.rows();
		Long2[] newValue = new Long2[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicIPAddrVector(newValue);
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
