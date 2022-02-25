package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicArrayVector extends AbstractVector {
	private DATA_TYPE type;
	private int[] rowIndices;
	private Vector valueVec;

	public BasicArrayVector(DATA_TYPE type, ExtendedDataInput in) throws IOException {
		super(DATA_FORM.DF_VECTOR);
		int rows = in.readInt();
		int cols = in.readInt(); 
		rowIndices = new int[rows];
		DATA_TYPE valueType = DATA_TYPE.valueOf(type.getValue() - 64);
		valueVec = BasicEntityFactory.instance().createVectorWithDefaultValue(valueType, cols);
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		
		int rowsRead = 0;
		int rowsReadInBlock = 0;
		int prevIndex = 0;
		int totalBytes = 0;
		while (rowsRead < rows) {
			//read block header
			int blockRows = in.readUnsignedShort();
			int countBytes = in.readUnsignedByte();
			in.skipBytes(1);

			//read array of counts
			totalBytes = blockRows * countBytes;
			rowsReadInBlock = 0;
			int offset = 0;
			while(offset < totalBytes){
				int len = Math.min(BUF_SIZE, totalBytes - offset);
				in.readFully(buf, 0, len);
				int curRows = len / countBytes;
				if(countBytes == 1){
					for (int i = 0; i < curRows; i++){
						int curRowCells = Byte.toUnsignedInt(buf[i]);
						rowIndices[rowsRead + rowsReadInBlock + i] = prevIndex + curRowCells;
						prevIndex += curRowCells;
					}
				}
				else if(countBytes == 2){
					ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
					for (int i = 0; i < curRows; i++){
						int curRowCells = Short.toUnsignedInt(byteBuffer.getShort(i * 2));
						rowIndices[rowsRead + rowsReadInBlock + i] = prevIndex + curRowCells;
						prevIndex += curRowCells;
					}
				}
				else {
					ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
					for (int i = 0; i < curRows; i++){
						int curRowCells = byteBuffer.getInt(i * 4);
						rowIndices[rowsRead + rowsReadInBlock + i] = prevIndex + curRowCells;
						prevIndex += curRowCells;
					}
				}
				rowsReadInBlock += curRows;
				offset += len;
			}
			
			//read array of values
			int rowStart =  rowsRead == 0 ? 0 : rowIndices[rowsRead - 1];
			int valueCount = rowIndices[rowsRead + rowsReadInBlock - 1] - rowStart;
			valueVec.deserialize(rowStart, valueCount, in);
			
			rowsRead += rowsReadInBlock;
		}
	}


	
	@Override
	public String getString(int index){
		StringBuilder sb = new StringBuilder("[");
		
		int startPosValueVec = index == 0 ? 0 : rowIndices[index - 1];
		int rows = rowIndices[index] - startPosValueVec;
		int size = Math.min(3, rows);
		if(size > 0)
			sb.append(valueVec.getString(startPosValueVec));
		for(int i=1; i<size; ++i){
			sb.append(',');
			sb.append(valueVec.getString(startPosValueVec + i));
		}
		if(size < rows)
			sb.append(",...");
		sb.append("]");
		return sb.toString();
	}
	
	@Override
	public Vector combine(Vector vector) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector getSubVector(int[] indices) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int asof(Scalar value) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isNull(int index) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setNull(int index) {
	}

	@Override
	public Scalar get(int index) {
		return new BasicString(getString(index));
	}

	@Override
	public void set(int index, Scalar value) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public Class<?> getElementClass() {
		return Entity.class;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.ARRAY;
	}

	@Override
	public DATA_TYPE getDataType() {
		return type;
	}

	@Override
	public int rows() {
		return rowIndices.length;
	}

	@Override
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
