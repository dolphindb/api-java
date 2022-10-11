package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;


public class BasicArrayVector extends AbstractVector {
	private DATA_TYPE type;
	private int[] rowIndices;
	private Vector valueVec;
	private int baseUnitLength_;
	private int rowIndicesSize;
	private int capacity;

	public BasicArrayVector(DATA_TYPE type, int size){
		super(DATA_FORM.DF_VECTOR);

		rowIndices = new int[size];
		if (type.getValue() == 81 || type.getValue() == 82)
			throw new RuntimeException("ArrayVector do not support String and Symbol");
		this.type = DATA_TYPE.valueOf(type.getValue());
		valueVec = BasicEntityFactory.instance().createVectorWithDefaultValue(DATA_TYPE.valueOf(type.getValue() - 64), 0);
		rowIndicesSize = 0;
		capacity = rowIndices.length;
	}

	public BasicArrayVector(List<Vector> value) throws Exception{
		super(DATA_FORM.DF_VECTOR);
		if (value.get(0).getDataType().getValue() + 64 == 81 || value.get(0).getDataType().getValue() + 64 == 82)
			throw new RuntimeException("ArrayVector do not support String and Symbol");
		else{
			this.type = DATA_TYPE.valueOf(value.get(0).getDataType().getValue() + 64);
			DATA_TYPE valueType = DATA_TYPE.valueOf(value.get(0).getDataType().getValue());
			int len = 0;
			for (Vector one : value){
				if(one.rows()>0)
					len += one.rows();
				else
					len++;
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
				if (size > 0){
					for (int i = 0; i < size ; i++){
						try {
							this.valueVec.set(index, temp.get(i));
							index++;
						}catch (Exception e){
							throw new RuntimeException("Failed to insert data, invalid data for "+((Vector)temp).get(i)+" error "+ e.toString());
						}
					}
					curRows += size;
				}
				else{
					this.valueVec.setNull(index);
					index++;
					curRows ++;
				}
				this.rowIndices[indexPos++] = curRows;
			}
			rowIndicesSize = rowIndices.length;
			capacity = rowIndices.length;
			this.baseUnitLength_ = (this.valueVec).getUnitLength();
		}
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
		rowIndicesSize = rowIndices.length;
		capacity = rowIndices.length;
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
		byte[] buf = new byte[4096];
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
				int len = Math.min(4096, totalBytes - offset);
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
		rowIndicesSize = rowIndices.length;
		capacity = rowIndices.length;
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
		if (indices.length == 0)
			return null;
		List<Vector> value = new ArrayList<>();
		for (int i = 0; i < indices.length; i++){
			int start = indices[i] == 0 ? 0 : rowIndices[indices[i]-1];
			int end = rowIndices[indices[i]];
			int[] indexs = new int[end - start];
			for (int j = 0; j < indexs.length; j++){
				indexs[j] = start+j;
			}
			Vector subValue = valueVec.getSubVector(indexs);
			value.add(subValue);
		}
		try {
			return new BasicArrayVector(value);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			return null;
		}
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
	public Entity get(int index) {
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

	public Vector getVectorValue(int index) {
		return (Vector) get(index);
	}

	@Override
	public void set(int index, Entity value) throws Exception {
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
		return rowIndicesSize;
	}

	@Override
	public int getUnitLength(){
		throw new RuntimeException("BasicArrayVector.getUnitLength() not supported.");
	}

	public void addRange(Object[] valueList) {
		throw new RuntimeException("ArrayVector not support addRange");
	}

	@Override
	public void Append(Scalar value) throws Exception{
		throw new RuntimeException("ArrayVector not support append scalar value");
	}

	@Override
	public void Append(Vector value) throws Exception{
		if (value.isVector()){
			int indexCount = rowIndicesSize;
			int prev = indexCount == 0? 0 : rowIndices[indexCount - 1];
			if (rowIndicesSize + 1 > capacity){
				rowIndices = Arrays.copyOf(rowIndices, rowIndices.length * 2);
				capacity = rowIndices.length;
			}
			if (value.rows() != 0){
				rowIndices[rowIndicesSize] = prev + value.rows();
				valueVec.Append(value);
			}else {
				rowIndices[rowIndicesSize] = prev + 1;
				Scalar scalar = BasicEntityFactory.instance().createScalarWithDefaultValue(value.getDataType());
				scalar.setNull();
				valueVec.Append(scalar);
			}
			rowIndicesSize++;
		}else
			throw new RuntimeException("Append to arrayctor must be a vector. ");
	}

	@Override
	public String getJsonString(int rowIndex) {
		StringBuilder sb = new StringBuilder("[");
		Vector value = (Vector) get(rowIndex);
		for (int j = 0; j < value.rows(); j++){
			sb.append(((Scalar)(value.get(j))).getJsonString());
			if (j != value.rows() - 1)
				sb.append(",");
		}
		sb.append("]");
		return sb.toString();
	}

	@Override
	public int serialize(int indexStart, int offect, int targetNumElement, NumElementAndPartial numElementAndPartial,  ByteBuffer out) throws IOException{
		numElementAndPartial.numElement = 0;
		numElementAndPartial.partial = 0;
		int byteSent = 0;
		NumElementAndPartial tempNumElementAndPartial = new NumElementAndPartial(0, 0);

		if(offect > 0)
		{
			int rowStart = indexStart > 0 ? rowIndices[indexStart - 1] : 0;
			int cellCount = rowIndices[indexStart] - rowStart;
			int cellCountToSerialize = Math.min(out.remaining() / baseUnitLength_, cellCount - offect);
			byteSent += valueVec.serialize(rowStart + offect, cellCount, numElementAndPartial.numElement, tempNumElementAndPartial, out);
			if(cellCountToSerialize < cellCount - offect)
			{
				numElementAndPartial.partial = offect + cellCountToSerialize;
				return byteSent;
			}
			else
			{
				--targetNumElement;
				++numElementAndPartial.numElement;
				++indexStart;
			}
		}

		int remainingBytes = out.remaining() - 4;
		int curCountBytes = 1;
		int maxCount = 255;
		int prestart = indexStart == 0 ? 0 : rowIndices[indexStart - 1];

		//one block can't exeed 65535 rows
		if (targetNumElement > 65535)
			targetNumElement = 65535;


		int i = 0;
		for(; i < targetNumElement && remainingBytes > 0; ++i)
		{
			int curStart = rowIndices[indexStart + i];
			int curCount = curStart - prestart;
			int oldCountBytes = curCountBytes;
			prestart = curStart;
			int byteRequired = 0;

			while(curCount > maxCount)
			{
				byteRequired += i * curCountBytes;
				curCountBytes *= 2;
				maxCount = Math.min(Integer.MAX_VALUE, (111 << (8 * curCountBytes)) - 1);
			}
			byteRequired += curCountBytes + curCount * baseUnitLength_;
			if(byteRequired > remainingBytes)
			{
				if(numElementAndPartial.numElement == 0)
				{
					numElementAndPartial.partial = (remainingBytes - curCountBytes) / baseUnitLength_;
					if(numElementAndPartial.partial <= 0)
					{
						numElementAndPartial.partial = 0;
					}
					else
					{
						++i;
						remainingBytes -= (curCountBytes + numElementAndPartial.partial * baseUnitLength_);
					}
				}
				else {
					if (oldCountBytes != curCountBytes)
						curCountBytes = oldCountBytes;
				}
				break;
			}
			else
			{
				remainingBytes -= byteRequired;
				++numElementAndPartial.numElement;
			}
		}

		if(i == 0)
			return byteSent;

		short rows = (short) i;
		out.putShort(rows);
		out.put((byte)curCountBytes);
		out.put((byte)0);
		byteSent += 4;

		//output array of counts
		prestart = indexStart == 0 ? 0 : rowIndices[indexStart - 1];
		for (int k = 0; k < rows; ++k)
		{
			int curStart = rowIndices[indexStart + k];
			int index = curStart - prestart;
			prestart = curStart;
			if (curCountBytes == 1)
				out.put((byte)index);
			else if (curCountBytes == 2)
				out.putShort((short)index);
			else
				out.putInt(index);
		}

		byteSent += curCountBytes * i;
		prestart = indexStart == 0 ? 0 : rowIndices[indexStart - 1];
		int count = (indexStart + numElementAndPartial.numElement == 0 ? 0 : rowIndices[indexStart + numElementAndPartial.numElement - 1]) + numElementAndPartial.partial - prestart;
		int bytes;
		bytes = valueVec.serialize(prestart, 0, count, tempNumElementAndPartial, out);
		return byteSent + bytes;
	}

	public void add(Object value) {
		throw new RuntimeException("ArrayVector not support add");
	}

	@Override
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
		// TODO Auto-generated method stub
		int indexCount = rowIndicesSize;
		int maxCount = 255;
		int countBytes = 1;
		int indicesPos = 0;
		int valuesOffect = 0;
		while (indicesPos < indexCount){
			int byteRequest = 4;
			int curRows = 0;
			int indiceCount = 1;
			while (byteRequest < 4096 && indicesPos + indiceCount - 1 < indexCount && indiceCount < 65536){// && indiceCount < 65536
				int curIndiceOffect = indicesPos + indiceCount - 1;
				int index = curIndiceOffect == 0 ? rowIndices[curIndiceOffect] : rowIndices[curIndiceOffect] - rowIndices[curIndiceOffect - 1];
				while(index > maxCount)
				{
					byteRequest += (indiceCount - 1) * countBytes;
					countBytes *= 2;
					maxCount = Math.min(Integer.MAX_VALUE, (111 << (8 * countBytes)) - 1);
				}
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
