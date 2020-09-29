package com.xxdb.data;

import java.io.IOException;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB string vector
 *
 */

public class BasicStringVector extends AbstractVector{
	private String[] values;
	private boolean isSymbol;
	private boolean isBlob = false;
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

	public BasicStringVector(List<String> list, boolean blob){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new String[list.size()];
			for (int i=0; i<list.size(); ++i)
				values[i] = list.get(i);
		}
		this.isSymbol = false;
		this.isBlob = blob;
	}

	public BasicStringVector(String[] array){
		super(DATA_FORM.DF_VECTOR);
		values = array.clone();
		this.isSymbol = false;
	}

	public BasicStringVector(String[] array, boolean blob){
		super(DATA_FORM.DF_VECTOR);
		values = array.clone();
		this.isSymbol = false;
		this.isBlob = blob;
	}

	protected BasicStringVector(DATA_FORM df, int size, boolean isSymbol){
		super(df);
		values = new String[size];
		this.isSymbol = isSymbol;
	}

	protected BasicStringVector(DATA_FORM df, int size, boolean isSymbol, boolean isBlob){
		super(df);
		values = new String[size];
		this.isBlob = isBlob;
		this.isSymbol = isSymbol;
	}

	protected BasicStringVector(DATA_FORM df, ExtendedDataInput in, boolean blob) throws IOException{
		super(df);
		isBlob = blob;
		int rows = in.readInt();
		int columns = in.readInt();
		int size = rows * columns;
		values = new String[size];
		if(!blob) {
			for (int i = 0; i < size; ++i)
				values[i] = in.readString();
		}else{
			for (int i = 0; i < size; ++i)
				values[i] = in.readBlob();
		}
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
	public int hashBucket(int index, int buckets){
		return BasicString.hashBucket(values[index], buckets);
	}

	@Override
	public Vector combine(Vector vector) {
		BasicStringVector v = (BasicStringVector)vector;
		int newSize = this.rows() + v.rows();
		String[] newValue = new String[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicStringVector(newValue);
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
		if(isBlob) return DATA_TYPE.DT_BLOB;
		return isSymbol ? DATA_TYPE.DT_SYMBOL : DATA_TYPE.DT_STRING;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicString.class;
	}

	@Override
	public int rows() {
		return values.length;
	}	
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		if(!isBlob) {
			for (String str : values)
				out.writeString(str);
		} else {
			for (String str : values)
				out.writeBlob(str);
		}
	}
}
