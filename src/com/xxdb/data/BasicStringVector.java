package com.xxdb.data;

import java.io.IOException;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicStringVector extends AbstractVector{
	private String[] values;
	private boolean isSymbol;
	
	public BasicStringVector(int size){
		this(DATA_FORM.DF_VECTOR, size, false);
	}
	
	public BasicStringVector(List<String> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new String[list.size()];
			for (int i=0; i<list.size(); ++i)
				values[i] = list.get(i);
		}
		this.isSymbol = false;
	}
	
	public BasicStringVector(String[] array){
		super(DATA_FORM.DF_VECTOR);
		values = array.clone();
		this.isSymbol = false;
	}
	
	protected BasicStringVector(DATA_FORM df, int size, boolean isSymbol){
		super(df);
		values = new String[size];
		this.isSymbol = isSymbol;
	}
	
	protected BasicStringVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int columns = in.readInt();
		int size = rows * columns;
		values = new String[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readString();
	}
	
	public Scalar get(int index){
		return new BasicString(values[index]);
	}
	
	public String getString(int index){
		return values[index];
	}
	
	public void set(int index, Scalar value) throws Exception {
		values[index] = value.getString();
	}
	
	public void setString(int index, String value){
		values[index] = value;
	}
	
	@Override
	public boolean isNull(int index) {
		return values[index] == null || values[index].isEmpty();
	}

	@Override
	public void setNull(int index) {
		values[index] = "";
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.LITERAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return isSymbol ? DATA_TYPE.DT_SYMBOL : DATA_TYPE.DT_STRING;
	}

	@Override
	public int rows() {
		return values.length;
	}	
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(String value : values)
			out.writeString(value);
	}
}
