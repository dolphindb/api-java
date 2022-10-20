package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB int vector
 *
 */

public class BasicIntVector extends AbstractVector{
	protected int[] values;
	protected int size;
	protected int capaticy;
	
	public BasicIntVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicIntVector(List<Integer> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new int[list.size()];
			for (int i=0; i<list.size(); ++i) {
				if(list.get(i)!=null)
					values[i] = list.get(i);
				else
					values[i] = Integer.MIN_VALUE;
			}
		}

		this.size = values.length;
		capaticy = values.length;
	}
	
	public BasicIntVector(int[] array){
		this(array, true);
	}
	
	public BasicIntVector(int[] array, boolean copy){
		super(DATA_FORM.DF_VECTOR);
		if(copy)
			values = array.clone();
		else
			values = array;

		this.size = values.length;
		capaticy = values.length;
	}
	
	protected BasicIntVector(DATA_FORM df, int size){
		super(df);
		values = new int[size];

		this.size = values.length;;
		capaticy = values.length;
	}
	
	protected BasicIntVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new int[size];
		int totalBytes = size * 4, off = 0;
		byte[] buf = new byte[4096];
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(4096, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = off / 4, end = len / 4;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getInt(i * 4);
			off += len;
		}

		this.size = values.length;
		capaticy = values.length;
	}
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		int totalBytes = count * 4, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		byte[] buf = new byte[4096];
		while (off < totalBytes) {
			int len = Math.min(4096, totalBytes - off);
			in.readFully(buf, 0, len);
			int end = len / 4;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getInt(i * 4);
			off += len;
			start += end;
		}

		this.size = values.length;
		capaticy = values.length;
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeInt(values[start + i]);
		}
	}

	public Entity get(int index){
		return new BasicInt(values[index]);
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		int[] sub = new int[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return new BasicIntVector(sub, false);
	}
	
	protected int[] getSubArray(int[] indices){
		int length = indices.length;
		int[] sub = new int[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return sub;
	}
	
	public int getInt(int index){
		return values[index];
	}
	
	public void set(int index, Entity value) throws Exception {
		if(((Scalar)value).isNull()){
			values[index] = Integer.MIN_VALUE;
		}else{
			values[index] = ((Scalar)value).getNumber().intValue();
		}
	}

	public void setInt(int index, int value){
		values[index] = value;
	}
	
	@Override
	public int hashBucket(int index, int buckets){
		int value = values[index];
		if(value >= 0)
			return value % buckets;
		else if(value == Integer.MIN_VALUE)
			return -1;
		else{
			return (int)((4294967296l + value) % buckets);
		}
	}

	@Override
	public int getUnitLength() {
		return 4;
	}

	public void add(int value) {
		if (size + 1 > capaticy && values.length > 0){
			values = Arrays.copyOf(values, values.length * 2);
		}else if (values.length <= 0){
			values = Arrays.copyOf(values, values.length + 1);
		}
		capaticy = values.length;
		values[size] = value;
		size++;
	}

	public void addRange(int[] valueList) {
		values = Arrays.copyOf(values, valueList.length + values.length);
		System.arraycopy(valueList, 0, values, size-1, valueList.length);
		size += valueList.length;
		capaticy = values.length;
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().intValue());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicIntVector)value).getdataArray());
	}

	public int[] getdataArray(){
		int[] data = new int[size];
		System.arraycopy(values, 0, data, 0, size);
		return data;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicIntVector v = (BasicIntVector)vector;
		int newSize = this.rows() + v.rows();
		int[] newValue = new int[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicIntVector(newValue);
	}

	@Override
	public boolean isNull(int index) {
		return values[index] == Integer.MIN_VALUE;
	}

	@Override
	public void setNull(int index) {
		values[index] = Integer.MIN_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.INTEGRAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_INT;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicInt.class;
	}

	@Override
	public int rows() {
		return size;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		int[] data = new int[size];
		System.arraycopy(values, 0, data, 0, size);
		out.writeIntArray(data);
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		int[] data = new int[size];
		System.arraycopy(values, 0, data, 0, size);
		for (int val: data) {
			buffer.putInt(val);
		}
		return buffer;
	}

	@Override
	public int asof(Scalar value) {
		int target;
		try{
			target = value.getNumber().intValue();
		}
		catch(Exception ex){
			throw new RuntimeException(ex);
		}
		
		int start = 0;
		int end = size - 1;
		int mid;
		while(start <= end){
			mid = (start + end)/2;
			if(values[mid] <= target)
				start = mid + 1;
			else
				end = mid - 1;
		}
		return end;
	}

	@Override
	public int serialize(int indexStart, int offect, int targetNumElement, NumElementAndPartial numElementAndPartial, ByteBuffer out) throws IOException{
		targetNumElement = Math.min((out.remaining() / getUnitLength()), targetNumElement);
		for (int i = 0; i < targetNumElement; ++i)
		{
			out.putInt(values[indexStart + i]);
		}
		numElementAndPartial.numElement = targetNumElement;
		numElementAndPartial.partial = 0;
		return targetNumElement * 4;
	}
}
