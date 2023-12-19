package com.xxdb.data;

import java.io.IOException;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB string matrix
 *
 */

public class BasicStringMatrix extends AbstractMatrix{
	private String[] values;
	private boolean isSymbol;
	
	public BasicStringMatrix(int rows, int columns){
		super(rows, columns);
		values = new String[rows * columns];
		isSymbol = true;
	}
	
	public BasicStringMatrix(int rows, int columns, List<String[]> list) throws Exception {
		super(rows,columns);
		values = new String[rows*columns];
		if (list == null || list.size() != columns)
			throw new Exception("input list of arrays does not have " + columns + " columns");
		for (int i=0; i<columns; ++i) {
			String[] array = list.get(i);
			if (array == null || array.length != rows)
				throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");
			System.arraycopy(array, 0, values, i*rows, rows);
		}
		isSymbol = true;
	}
	
	public BasicStringMatrix(ExtendedDataInput in) throws IOException {
		super(in);
		isSymbol = true;
	}

	public void setString(int row, int column, String value){
		values[getIndex(row, column)] = value;
	}

	@JsonIgnore
	@Override
	public String getString() {
		return super.getString();
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
	public Class<?> getElementClass(){
		return BasicString.class;
	}

	@Override
	protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
		int size = rows * columns;
		values =new String[size];
		if(extended){
			SymbolBase symbase = new SymbolBase(in);
			for(int i=0; i<size; ++i)
				values[i] = symbase.getSymbol(in.readInt());
		}
		else{
			for(int i=0; i<size; ++i)
				values[i] = in.readString();
		}
	}

	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(String value : values)
			out.writeString(value, isSymbol);
	}
}
