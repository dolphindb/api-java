package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;


public class BasicArrayVector extends AbstractVector {
	private DATA_TYPE type;
	private int[] rowIndices;
	private Vector valueVec;
	private int baseUnitLength_;

	public BasicArrayVector(List<Vector> value) {
		super(DATA_FORM.DF_VECTOR);
		this.type = DATA_TYPE.valueOf(value.get(0).getDataType().getValue() + 64);
		DATA_TYPE valueType = DATA_TYPE.valueOf(value.get(0).getDataType().getValue());
		int len = 0;
		for (Vector one : value){
			len += one.rows();
		}
		int indexPos = 0;
		int indexCount = value.size();
		this.rowIndices = new int[indexCount];
		this.valueVec = BasicEntityFactory.instance().createVectorWithDefaultValue(valueType, len);
		int index = 0;
		int curRows = 0;
		for (int valuePos = 0; valuePos < indexCount; valuePos++){
			Vector temp = value.get(valuePos);
			int size = temp.rows();
			for (int i = 0; i < size ; i++){
				try {
					this.valueVec.set(index, temp.get(i));
					index++;
				}catch (Exception e){
					throw new RuntimeException("Failed to insert data, invalid data for "+((Vector)temp).get(i)+" error "+ e.toString());
				}
			}
			curRows += size;
			this.rowIndices[indexPos++] = curRows;
		}
		this.baseUnitLength_ = (this.valueVec).getUnitLength();
	}

	public BasicArrayVector(int[] index, Vector value) {
		super(DATA_FORM.DF_VECTOR);
		DATA_TYPE dataType = value.getDataType();
		this.type = DATA_TYPE.valueOf(dataType.getValue() + 64);
		this.valueVec = value;
		int indexCount = index.length;
		rowIndices = new int[indexCount];
		System.arraycopy(index, 0, this.rowIndices, 0, indexCount);
		this.baseUnitLength_ = value.getUnitLength();
	}

	public BasicArrayVector(DATA_TYPE type, ExtendedDataInput in) throws IOException {
		super(DATA_FORM.DF_VECTOR);
		this.type = type;
		int rows = in.readInt();
		int cols = in.readInt(); 
		rowIndices = new int[rows];
		DATA_TYPE valueType = DATA_TYPE.valueOf(type.getValue() - 64);
		valueVec = BasicEntityFactory.instance().createVectorWithDefaultValue(valueType, cols);
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		this.baseUnitLength_ = valueVec.getUnitLength();
		
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
		return valueVec.get(index);
	}

	public Vector getVectorValue(int index) {
		int startPosValueVec = index == 0 ? 0 : rowIndices[index - 1];
		int rows = rowIndices[index] - startPosValueVec;
		DATA_TYPE valueType = DATA_TYPE.valueOf(type.getValue()-64);
		Vector value = BasicEntityFactory.instance().createVectorWithDefaultValue(valueType, rows);
		if (rows > 0){
			try {
				value.set(0, valueVec.get(startPosValueVec));
			}catch (Exception e){
				throw new RuntimeException("Failed to insert data, invalid data for "+valueVec.get(startPosValueVec)+" error "+ e.toString());
			}
		}
		for (int i = 1;i<rows;i++){
			try {
				value.set(i, valueVec.get(startPosValueVec + i));
			}catch (Exception e){
				throw new RuntimeException("Failed to insert data, invalid data for "+valueVec.get(startPosValueVec)+" error "+ e.toString());
			}
		}
		return value;
	}

	@Override
	public void set(int index, Scalar value) throws Exception {
		// TODO Auto-generated method stub
		throw new RuntimeException("BasicArrayVector.set not supported.");
	}

	@Override
	public Class<?> getElementClass() {
		return Entity.class;
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		throw new RuntimeException("BasicAnyVector.serialize not supported.");
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
	public int getUnitLength(){
		throw new RuntimeException("BasicArrayVector.getUnitLength() not supported.");
	}

	@Override
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
		// TODO Auto-generated method stub

		int indexCount = rowIndices.length;
		int cols = valueVec.rows();
		int maxCount = 255;
		int countBytes = 1;
		int indicesPos = 0;
		int valuesOffect = 0;

		while (indicesPos < indexCount){
			int byteRequest = 4;
			int curRows = 0;
			int indiceCount = 1;
			while (byteRequest < BUF_SIZE && indicesPos + indiceCount - 1 < indexCount){
				int curIndiceOffect = indicesPos + indiceCount - 1;
				int index = curIndiceOffect == 0 ? rowIndices[curIndiceOffect] : rowIndices[curIndiceOffect] - rowIndices[curIndiceOffect - 1];
				while(index > maxCount)
				{
					byteRequest += (indiceCount - 1) * countBytes;
					countBytes *= 2;
					if (countBytes == 1)
						maxCount = Byte.MAX_VALUE;
					else if (countBytes == 2)
						maxCount = Short.MAX_VALUE;
					else if (countBytes == 3)
						maxCount = Integer.MAX_VALUE;
				}
				if (byteRequest + countBytes + baseUnitLength_ * index > BUF_SIZE)
					break;
				curRows += index;
				indiceCount++;
				byteRequest += countBytes + baseUnitLength_ * index;
			}
			indiceCount --;
			out.writeShort(indiceCount);
			out.writeByte(countBytes);
			out.writeByte(0);
			for (int i = 0; i < indiceCount; i++){
				int index = indicesPos + i == 0 ? rowIndices[indicesPos + i] : rowIndices[indicesPos + i] - rowIndices[indicesPos + i - 1];
				if (countBytes == 1)
					out.writeByte(index);
				else if (countBytes == 2)
					out.writeShort(index);
				else
					out.writeInt(index);
			}
			valueVec.serialize(valuesOffect, curRows, out);
			indicesPos += indiceCount;
			valuesOffect += curRows;
		}
	}

}
