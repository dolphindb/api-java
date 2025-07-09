package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.route.BitConverter;

/**
 *
 * Corresponds to DolphinDB string vector
 *
 */

public class BasicStringVector extends AbstractVector{
	private String[] values;
	private boolean isSymbol;
	private boolean isBlob = false;
	private List<byte[]> blobValues;
	private int size;
	private int capaticy;
    private static final String SYMBOL_LENGTH_EXCEED_MSG = "Serialized symbol length must be less than 256k bytes.";
    private static final String STRING_LENGTH_EXCEED_MSG = "Serialized string length must be less than 64k bytes.";
	private static final String BLOB_LENGTH_EXCEED_MSG = "Serialized blob length must be less than 64m bytes.";
	private static final int SERIALIZED_SYMBOL_MAX_LENGTH = 262144;
	private static final int SERIALIZED_STRING_MAX_LENGTH = 65536;
	private static final int SERIALIZED_BLOB_MAX_LENGTH = 67108864;

	public BasicStringVector(int size){
		this(DATA_FORM.DF_VECTOR, size, false);
	}
	
	public BasicStringVector(List<String> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new String[list.size()];
			for (int i=0; i<list.size(); ++i) {
				if(list.get(i) != null) {
					values[i] = list.get(i);
				}else{
					values[i] = "";
				}
			}
		}
		this.isSymbol = false;

		this.size = values.length;
		capaticy = values.length;
	}

	public BasicStringVector(List<String> list, boolean blob) {
		super(DATA_FORM.DF_VECTOR);
		if (blob){
			int rows = list.size();
			this.blobValues = new ArrayList<>();
			for (int i = 0; i < rows; ++i){
				if(list.get(i) != null){
					this.blobValues.add(list.get(i).getBytes(StandardCharsets.UTF_8));
				}else{
					this.blobValues.add("".getBytes(StandardCharsets.UTF_8));
				}
			}
		}
		if (list != null) {
			values = new String[list.size()];
			for (int i=0; i<list.size(); ++i) {
				if(list.get(i) != null) {
					values[i] = list.get(i);
				}else{
					values[i] = "";
				}
			}
		}
		this.isSymbol = false;
		this.isBlob = blob;

		if (blob) {
			this.size = blobValues.size();
			capaticy = blobValues.size();
		} else {
			this.size = values.length;
			capaticy = values.length;
		}
	}

	public BasicStringVector(byte[][] array) {
		super(DATA_FORM.DF_VECTOR);
		blobValues = new ArrayList<>(array.length);
		List<byte[]> arraycopy = new ArrayList<>();
		for (int i = 0; i < array.length; i++) {
			arraycopy.add(array[i]);
		}
		blobValues.addAll(arraycopy);
		isBlob = true;

		this.size = blobValues.size();
		capaticy = blobValues.size();
	}

	public BasicStringVector(String[] array){
		this(array, false, true);
	}

	public BasicStringVector(String[] array, boolean blob){
		this(array, blob, true);
	}
	
	public BasicStringVector(String[] array, boolean blob, boolean copy){
		super(DATA_FORM.DF_VECTOR);
		if (blob){
			blobValues = new ArrayList<>(array.length);
			if (copy) {
				String[] valuecopy = array.clone();
				for (int i = 0; i < valuecopy.length; i++) {
					if(valuecopy[i] != null){
						this.blobValues.add(valuecopy[i].getBytes(StandardCharsets.UTF_8));
					}else{
						this.blobValues.add("".getBytes(StandardCharsets.UTF_8));
					}
				}
			} else {
				values = array;
				for (int i = 0; i < values.length; i++){
					if(array[i] != null){
						this.blobValues.add(array[i].getBytes(StandardCharsets.UTF_8));
					}else {
						this.blobValues.add("".getBytes(StandardCharsets.UTF_8));
					}

				}
			}
		}else {
			if(copy)
				values = array.clone();
			else
				values = array;
			for(int i = 0; i < values.length; i++){
				if(values[i] == null)
					values[i] = "";
			}
		}

		this.isSymbol = false;
		this.isBlob = blob;

		if (blob){
			this.size = blobValues.size();
			capaticy = blobValues.size();
		}else {
			this.size = values.length;
			capaticy = values.length;
		}
	}

	protected BasicStringVector(DATA_FORM df, int size, boolean isSymbol){
		super(df);
		values = new String[size];
		this.isSymbol = isSymbol;
		this.size = values.length;
		capaticy = values.length;
	}

	protected BasicStringVector(DATA_FORM df, int size, boolean isSymbol, boolean isBlob){
		super(df);
		if (isBlob) {
			blobValues = new ArrayList<>(size);
			for (int i = 0; i < size; i++){
				blobValues.add(null);
			}
		}
		else {
			values = new String[size];
		}
		this.isSymbol = isSymbol;
		this.isBlob = isBlob;
		if (isBlob){
			this.size = blobValues.size();
			capaticy = blobValues.size();
		}else {
			this.size = values.length;
			capaticy = values.length;
		}
	}

	protected BasicStringVector(DATA_FORM df, ExtendedDataInput in, boolean isSymbol, boolean blob) throws IOException {
		super(df);
		this.isBlob = blob;
		this.isSymbol = isSymbol;
		int rows = in.readInt();
		int columns = in.readInt();
		if (blob) {
			blobValues = new ArrayList<>(rows);
			for(int i = 0; i < rows; ++i)
			{
				blobValues.add(in.readBlob());
			}
		} else {
			values = new String[rows];
			for (int i = 0; i < rows; ++i) {
				values[i] = in.readString();
			}
		}

		if (blob) {
			this.size = blobValues.size();
			capaticy = blobValues.size();
		} else {
			this.size = values.length;
			capaticy = values.length;
		}
	}
	
	protected BasicStringVector(DATA_FORM df, ExtendedDataInput in, boolean blob, SymbolBaseCollection collection) throws IOException{
		super(df);
		isBlob = blob;
		int rows = in.readInt();
		int columns = in.readInt();
		if(!blob) {
			values = new String[rows];
			if(collection != null){
				SymbolBase symbase = collection.add(in);
				for (int i = 0; i < rows; ++i){
					values[i] = symbase.getSymbol(in.readInt());
				}
			} else {
				for (int i = 0; i < rows; ++i)
					values[i] = in.readString();
			}
		} else {
			blobValues = new ArrayList<>(rows);
			for (int i = 0; i < rows; ++i)
				blobValues.add(in.readBlob());
		}

		if (blob) {
			this.size = blobValues.size();
			capaticy = blobValues.size();
		} else {
			this.size = values.length;
			capaticy = values.length;
		}
	}
	
	public Entity get(int index){
		if (isBlob)
			return new BasicString(blobValues.get(index), true);
		else
			return new BasicString(values[index], false);
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		if (isBlob) {
			String[] subBlobV = new String[length];
			for (int i = 0; i < length; i++){
				subBlobV[i] = new String(blobValues.get(indices[i]), StandardCharsets.UTF_8);
			}
			return new BasicStringVector(subBlobV, true, false);
		} else {
			String[] sub = new String[length];
			for(int i=0; i<length; ++i)
				sub[i] = values[indices[i]];
			return new BasicStringVector(sub, false, false);
		}
	}
	
	@Override
	public String getString(int index) {
		if (isBlob) {
			return new String(blobValues.get(index), StandardCharsets.UTF_8);
		} else {
			if (isNull(index))
				return "";
			else
				return values[index];
		}
	}

	@Override
	public int getUnitLength() {
		return 1;
	}


	public void add(String value) {
		if (isBlob) {
			blobValues.add(value.getBytes(StandardCharsets.UTF_8));
			capaticy = blobValues.size();
		} else {
			if (size + 1 > capaticy && values.length > 0){
				values = Arrays.copyOf(values, values.length * 2);
			}else if (values.length <= 0){
				values = Arrays.copyOf(values, values.length + 1);
			}
			capaticy = values.length;
			values[size] = value;
		}
		size++;
	}


	public void addRange(String[] valueList) {
		if (isBlob) {
			for (int i = 0; i < valueList.length; i++)
				blobValues.add(valueList[i].getBytes(StandardCharsets.UTF_8));

			size += valueList.length;
			capaticy = blobValues.size();
		} else {
			checkCapacity(size + valueList.length);
			System.arraycopy(valueList, 0, values, size, valueList.length);
			size += valueList.length;
		}
	}

	@Override
	public void Append(Scalar value) {
		if (isBlob) {
			blobValues.add(((BasicString)value).getBytes());
			size++;
			capaticy = blobValues.size();
		} else {
			add(((BasicString)value).getString());
		}
	}

	@Override
	public void Append(Vector value) {
		if (isBlob){
			blobValues.addAll(((BasicStringVector)value).getdataByteArray());
			size += value.rows();
			capaticy = blobValues.size();
		} else {
			addRange(((BasicStringVector)value).getdataArray());
		}
	}

	@Override
	public void checkCapacity(int requiredCapacity) {
		if (requiredCapacity > values.length) {
			int newCapacity = Math.max(
					(int)(values.length * GROWTH_FACTOR),
					requiredCapacity
			);
			values = Arrays.copyOf(values, newCapacity);
			capaticy = newCapacity;
		}
	}

	@JsonIgnore
	public String[] getdataArray() {
		String[] data = new String[size];
		System.arraycopy(values, 0, data, 0, size);
		return data;
	}

	public List<byte[]> getdataByteArray(){
		return blobValues;
	}

	public void set(int index, Entity value) throws Exception {
		if (isBlob) {
			if (value.getDataType() == DATA_TYPE.DT_BLOB) {
				blobValues.set(index, ((BasicString)value).getBytes());
			} else
				throw new Exception("The value must be a blob scalar. ");
		} else {
			if (value.getDataType() == DATA_TYPE.DT_STRING) {
				values[index] = ((BasicString)value).getString();
			} else
				throw new Exception("The value must be a string scalar. ");
		}
	}

	public void setString(int index, String value){
		if (isBlob) {
			blobValues.set(index, value.getBytes(StandardCharsets.UTF_8));
		} else {
			values[index] = value;
		}
	}
	
	@Override
	public int hashBucket(int index, int buckets){
		return ((Scalar)get(index)).hashBucket(buckets);
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
		if (isBlob)
			return blobValues.get(index).length == 0;
		else
			return values[index] == null || values[index].length() == 0;
	}

	@Override
	public void setNull(int index) {
		if (isBlob)
			blobValues.set(index, new byte[0]);
		else
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
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		if (isBlob) {
			for (int i = 0; i < count; ++i)
				out.writeBlob(blobValues.get(start + i));
		} else {
			for (int i = 0; i < count; ++i)
				out.writeString(values[start + i]);
		}
	}

	@Override
	public int rows() {
		return size;
	}	
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		if (isBlob) {
			for (byte[] str : blobValues) {
				out.writeBlob(str);
			}
		} else {
			String[] data = new String[size];
			System.arraycopy(values, 0, data, 0, size);
			for (String str : data) {
				if (str == null)
					out.writeString("");
				else
					out.writeString(str);
			}
		}
	}

	private static int compare(byte[] b1, byte[] b2) {
		int minLength = Math.min(b1.length, b2.length);
		for(int i = 0; i < minLength; ++i) {
			if (b1[i] < b2[i])
				return -1;
			if (b1[i] > b2[i])
				return 1;
		}
		if (b1.length > b2.length)
			return 1;
		else if (b1.length == b2.length)
			return 0;
		else
			return -1;
	}
	
	@Override
	public int asof(Scalar value) {
		if (isBlob) {
			if (value.getDataType() != DATA_TYPE.DT_BLOB)
				throw new RuntimeException("value must be a blob scalar. ");
			byte[] target = ((BasicString)value).getBytes();
			int start = 0;
			int end = size - 1;
			int mid;
			while (start <= end) {
				mid = (start + end) / 2;

				if (compare(blobValues.get(mid), target) <= 0)
					start = mid + 1;
				else
					end = mid - 1;
			}
			return end;
		} else {
			String target = value.getString();
			int start = 0;
			int end = size - 1;
			int mid;
			while (start <= end)
			{
				mid = (start + end) / 2;

				if (values[mid].compareTo(target) <= 0)
					start = mid + 1;
				else
					end = mid - 1;
			}
			return end;
		}
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		if (isBlob) {
			for (byte[] val : blobValues) {
				if (val.length >= SERIALIZED_BLOB_MAX_LENGTH)
					throw new RuntimeException(BLOB_LENGTH_EXCEED_MSG);

				while(val.length + 1 + buffer.position() > buffer.limit()) {
					buffer = Utils.reAllocByteBuffer(buffer, Math.max(buffer.capacity() * 2,1024));
				}
				buffer.putInt(val.length);
				buffer.put(val);
			}
		} else {
			String[] data = new String[size];
			System.arraycopy(values, 0, data, 0, size);
			for (String val : data) {
				if(val.length() >= 262144) {
					throw new RuntimeException("Serialized string length must" +
							" less than 256k bytes.");
				}

				byte[] tmp = val.getBytes(StandardCharsets.UTF_8);
				while(tmp.length + 1 + buffer.position() > buffer.limit()) {
					buffer = Utils.reAllocByteBuffer(buffer, Math.max(buffer.capacity() * 2,1024));
				}
				buffer.put(tmp, 0, tmp.length);
				buffer.put((byte)0);
			}
		}
		return buffer;
	}

	public int serialize(byte[] buf, int bufSize, int start, int elementCount, Offect offect) {
		int len = 0;
		int total = size;
		int count = 0;
		if (isBlob) {
			while(len < bufSize && count < elementCount && start + count < total) {
				byte[] str = blobValues.get(start + count);
				int strLen = str.length;
				if (strLen >= SERIALIZED_BLOB_MAX_LENGTH)
					throw new RuntimeException(BLOB_LENGTH_EXCEED_MSG);

				if (strLen + Integer.BYTES + 1 >= bufSize - len)
					break;
				byte[] lenTmp = BitConverter.getBytes(strLen);
				System.arraycopy(lenTmp, 0, buf, len, Integer.BYTES);
				System.arraycopy(str, 0, buf, len + Integer.BYTES, strLen);
				len += strLen + Integer.BYTES;
				count++;
			}
		} else {
			while (len < bufSize && count < elementCount && start + count < total) {
				if(values[start + count].length() >= 262144) {
					throw new RuntimeException("Serialized string length must" +
							" less than 256k bytes.");
				}

				String str = values[start + count];
				int strLen = str.length();
				if (strLen + 1 >= bufSize - len)
					break;
				byte[] strTmp = str.getBytes(Charset.defaultCharset());
				System.arraycopy(strTmp, 0, buf, len, strLen);
				len += strLen + 1;
				buf[len - 1] = 0;
				count++;
			}
		}
		offect.offect = start + count;
		return len;
	}

	@Override
	public int serialize(int indexStart, int offect, int targetNumElement, NumElementAndPartial numElementAndPartial, ByteBuffer out)throws IOException {
		int readByte = 0;
		targetNumElement = Math.min(targetNumElement, isBlob ? blobValues.size() - indexStart: values.length - indexStart);
		numElementAndPartial.numElement = 0;
		for (int i = 0; i < targetNumElement; ++i, ++numElementAndPartial.numElement) {
			if (isBlob) {
				byte[] data = blobValues.get(i);
				if (data.length >= SERIALIZED_BLOB_MAX_LENGTH)
					throw new RuntimeException(BLOB_LENGTH_EXCEED_MSG);

				if (Integer.BYTES + data.length > out.remaining())
					break;
				out.putInt(data.length);
				out.put(data);
				readByte += Integer.BYTES + data.length;
			} else {
				if(values[indexStart + i].length() >= 262144) {
					throw new RuntimeException("Serialized string length must" +
							" less than 256k bytes.");
				}

				byte[] data = values[indexStart + i].getBytes(Charset.defaultCharset());
				if (Byte.BYTES + data.length > out.remaining())
					break;
				out.put(data);
				out.put((byte)0);
				readByte += Byte.BYTES + data.length;
			}
		}
		numElementAndPartial.partial = 0;
		if(numElementAndPartial.numElement == 0)
			throw new RuntimeException("too large data. ");
		return readByte;
	}

	public String[] getValues() {
		return values;
	}
}
