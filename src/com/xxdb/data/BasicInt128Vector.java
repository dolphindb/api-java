package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.Long2;

public class BasicInt128Vector extends AbstractVector{
	protected Long2[] values;
	
	public BasicInt128Vector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicInt128Vector(List<Long2> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new Long2[list.size()];
			for (int i=0; i<list.size(); ++i)
				values[i] = list.get(i);
		}
	}
	
	public BasicInt128Vector(Long2[] array){
		super(DATA_FORM.DF_VECTOR);
		values = array.clone();
	}
	
	protected BasicInt128Vector(DATA_FORM df, int size){
		super(df);
		values = new Long2[size];
		for(int i=0; i<size; ++i)
			values[i] = new Long2(0, 0);
	}
	
	protected BasicInt128Vector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new Long2[size];
		int totalBytes = size * 16, off = 0;
		boolean littleEndian = in.isLittleEndian();
		ByteOrder bo = littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = off / 16, end = len / 16;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			if(littleEndian){
				for (int i = 0; i < end; i++){
					long low = byteBuffer.getLong(i * 16);
					long high = byteBuffer.getLong(i * 16 + 8);
					values[i + start] = new Long2(high, low);
				}
			}
			else{
				for (int i = 0; i < end; i++){
					long high = byteBuffer.getLong(i * 16);
					long low = byteBuffer.getLong(i * 16 + 8);
					values[i + start] = new Long2(high, low);
				}
			}
			off += len;
		}
	}
	
	public Scalar get(int index){
		return new BasicInt128(values[index].high, values[index].low);
	}
	
	public void set(int index, Scalar value) throws Exception {
		values[index].high = ((BasicInt128)value).getMostSignicantBits();
		values[index].low = ((BasicInt128)value).getLeastSignicantBits();
	}
	
	public void setInt128(int index, long highValue, long lowValue){
		values[index].high = highValue;
		values[index].low = lowValue;
	}
	
	@Override
	public int hashBucket(int index, int buckets){
		return values[index].hashBucket(buckets);
	}

	@Override
	public Vector combine(Vector vector) {
		BasicInt128Vector v = (BasicInt128Vector)vector;
		int newSize = this.rows() + v.rows();
		Long2[] newValue = new Long2[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicInt128Vector(newValue);
	}

	@Override
	public boolean isNull(int index) {
		return values[index].isNull();
	}

	@Override
	public void setNull(int index) {
		values[index].setNull();
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.BINARY;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_INT128;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicInt128.class;
	}

	@Override
	public int rows() {
		return values.length;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeLong2Array(values);
	}
}

