package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB float matrix
 *
 */

public class BasicFloatMatrix extends AbstractMatrix{
	private float[] values;
	
	public BasicFloatMatrix(int rows, int columns){
		super(rows, columns);
		values = new float[rows * columns];
	}
	
	public BasicFloatMatrix(int rows, int columns, List<float[]> listOfArrays) throws Exception {
		super(rows,columns);
		values = new float[rows*columns];
		if (listOfArrays == null || listOfArrays.size() != columns)
			throw new Exception("input list of arrays does not have " + columns + " columns");
		for (int i=0; i<columns; ++i) {
			float[] array = listOfArrays.get(i);
			if (array == null || array.length != rows)
				throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");
			System.arraycopy(array, 0, values, i*rows, rows);
		}
	}
	
	public BasicFloatMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setFloat(int row, int column, float value){
		values[getIndex(row, column)] = value;
	}
	
	public float getFloat(int row, int column){
		return values[getIndex(row, column)];
	}
	
	@Override
	public boolean isNull(int row, int column) {
		return values[getIndex(row, column)] == -Float.MAX_VALUE;
	}

	@Override
	public void setNull(int row, int column) {
		values[getIndex(row, column)] =  -Float.MAX_VALUE;
	}

	@Override
	public Scalar get(int row, int column) {
		return new BasicFloat(values[getIndex(row, column)]);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.FLOATING;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_FLOAT;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicFloat.class;
	}

	@Override
	protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
		int size = rows * columns;
		values =new float[size];
		long totalBytes = (long)size * 4, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = (int)Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = (int)(off / 4), end = len / 4;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getFloat(i * 4);
			off += len;
		}
	}

	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(float value : values)
			out.writeFloat(value);
	}

	@JsonIgnore
	@Override
	public int getScale() {
		return super.getScale();
	}
}
