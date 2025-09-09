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
 * Corresponds to DolphinDB symbol vector
 *
 */
public class BasicSymbolVector extends AbstractVector {
	private SymbolBase base;
	private int[] values;
	private int size;
	private int capacity;
	
	public BasicSymbolVector(int size){
		super(DATA_FORM.DF_VECTOR);
		base = new SymbolBase(0);
		base.find("", true);
		values = new int[size];

		this.size = values.length;
		capacity = values.length;
	}

	public BasicSymbolVector(int size, int capacity) {
		super(DATA_FORM.DF_VECTOR);
		if (capacity < size) {
			capacity = size;
		}

		this.base = new SymbolBase(0);
		this.base.find("", true);
		this.values = new int[capacity];
		this.size = size;
		this.capacity = capacity;
	}
	
	public BasicSymbolVector(SymbolBase base, int size){
		super(DATA_FORM.DF_VECTOR);
		this.base = base;
		values = new int[size];

		this.size = values.length;
		capacity = values.length;
	}
	
	public BasicSymbolVector(List<String> list){
		super(DATA_FORM.DF_VECTOR);
		base = new SymbolBase(0);
		values = new int[list.size()];
		for (int i=0; i<list.size(); ++i) {
			if(list.get(i) != null)
				values[i] = base.find(list.get(i), true);
			else
				values[i] = 0;
		}

		this.size = values.length;
		capacity = values.length;
	}
	
	public BasicSymbolVector(SymbolBase base, int[] values, boolean copy){
		super(DATA_FORM.DF_VECTOR);
		this.base  = base;
		if(copy){
			this.values = new int[values.length];
			System.arraycopy(values, 0, this.values, 0, values.length);
		}
		else{
			this.values = values;
		}

		this.size = values.length;
		capacity = values.length;
	}

	protected BasicSymbolVector(DATA_FORM df, ExtendedDataInput in) throws IOException {
		super(df);
		int rows = in.readInt();
		int columns = in.readInt();
		values = new int[rows];
		base = new SymbolBase(in);
		int totalBytes = rows * 4, off = 0;
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
		capacity = values.length;
	}
	
	protected BasicSymbolVector(DATA_FORM df, ExtendedDataInput in, SymbolBaseCollection collection) throws IOException{
		super(df);
		int rows = in.readInt();
		int columns = in.readInt();
		values = new int[rows];
		base = collection.add(in);
		int totalBytes = rows * 4, off = 0;
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
		capacity = values.length;
	}
	
	public Entity get(int index){
		return new BasicString(base.getSymbol(values[index]));
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		int[] sub = new int[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return new BasicSymbolVector(base, sub, false);
	}
	
	@Override
	public String getString(int index){
		return base.getSymbol(values[index]);
	}

	@Override
	public int getUnitLength() {
		return 4;
	}

	@Override
	public void add(Object value) {
		add((String) value);
	}

	public void add(String value) {
		throw new RuntimeException("SymbolVector does not support add");
	}


	public void addRange(String[] valueList) {
		throw new RuntimeException("SymbolVector does not support addRange");
	}

	@Override
	public void Append(Scalar value) {
		if (size + 1 > capacity){
			values = Arrays.copyOf(values, values.length * 2);
			capacity = values.length;
		}
		values[size] = base.find(value.getString(), true);
		size++;
	}

	@Override
	public void Append(Vector value) {
		throw new RuntimeException("SymbolVector does not support append a vector");
	}

	@Override
	public void checkCapacity(int requiredCapacity) {
		throw new RuntimeException("BasicSymbolVector not support checkCapacity.");
	}

	public void set(int index, Entity value) throws Exception {
		if (value == null) {
			setNull(index);
		} else {
			values[index] = base.find(value.getString(), true);
		}
	}

	@Override
	public void set(int index, Object value) {
		if (value == null) {
			setNull(index);
		} else if (value instanceof String) {
			setString(index, (String) value);
		} else {
			throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only String or null is supported.");
		}
	}

	public void setString(int index, String value) {
		if (value == null) {
			setNull(index);
		} else {
			values[index] = base.find(value, true);
		}
	}
	
	@Override
	public int hashBucket(int index, int buckets){
		return BasicString.hashBucket(base.getSymbol(values[index]), buckets);
	}

	@Override
	public Vector combine(Vector vector) {
		BasicSymbolVector v = (BasicSymbolVector)vector;
		int newSize = this.rows() + v.rows();
		int[] newValue = new int[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		if(v.base == base)
			System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		else{
			SymbolBase vBase = v.base;
			int length = vBase.size();
			int[] mapper = new int[length];
			for(int i=0; i<length; ++i)
				mapper[i] = base.find(vBase.getSymbol(i), true);
			length = v.rows();
			int[] vValues = v.values;
			int baseRow = this.rows();
			for(int i=0; i<length; ++i){
				newValue[baseRow + i] = mapper[vValues[i]];
			}
		}
		return new BasicSymbolVector(base, newValue, false);
	}

	@Override
	public boolean isNull(int index) {
		return values[index] == 0;
	}

	@Override
	public void setNull(int index) {
		values[index] = 0;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.LITERAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_SYMBOL;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicString.class;
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeInt(values[start + i]);
		}
	}

	@Override
	public int rows() {
		return size;
	}	
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		int[] data = new int[size];
		System.arraycopy(values, 0, data, 0, size);
		base.write(out);
		out.writeIntArray(data);
	}
	
	public void write(ExtendedDataOutput out, SymbolBaseCollection collection) throws IOException{
		int dataType = getDataType().getValue() + 128;
		int flag = (DATA_FORM.DF_VECTOR.ordinal() << 8) + dataType;
		int[] data = new int[size];
		System.arraycopy(values, 0, data, 0, size);
		out.writeShort(flag);
		out.writeInt(rows());
		out.writeInt(columns());
		collection.write(out, base);
		out.writeIntArray(data);
	}
	
	@Override
	public int asof(Scalar value) {
		String target = value.getString();
		int start = 0;
		int end = size - 1;
		int mid;
		while(start <= end){
			mid = (start + end)/2;
			if(base.getSymbol(values[mid]).compareTo(target) <= 0)
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

	public int[] getValues() {
		return values;
	}
}
