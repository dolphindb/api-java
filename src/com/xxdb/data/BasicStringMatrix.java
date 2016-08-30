package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicStringMatrix extends AbstractMatrix{
	private String[] values;
	private boolean isSymbol;
	
	public BasicStringMatrix(int rows, int columns){
		super(rows, columns);
		values = new String[rows * columns];
		isSymbol = true;
	}
	
	public BasicStringMatrix(ExtendedDataInput in) throws IOException {
		super(in);
		isSymbol = true;
	}

	public void setString(int row, int column, String value){
		values[getIndex(row, column)] = value;
	}
	
	public String getString(int row, int column){
		return values[getIndex(row, column)];
	}
	
	@Override
	public boolean isNull(int row, int column) {
		return values[getIndex(row, column)].isEmpty();
	}

	@Override
	public void setNull(int row, int column) {
		values[getIndex(row, column)] = "";
	}

	@Override
	public Scalar get(int row, int column) {
		return new BasicString(values[getIndex(row, column)]);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.LITERAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return isSymbol ? DATA_TYPE.DT_SYMBOL : DATA_TYPE.DT_STRING;
	}

	@Override
	protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
		int size = rows * columns;
		values =new String[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readString();
	}

	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(String value : values)
			out.writeString(value);
	}
}
