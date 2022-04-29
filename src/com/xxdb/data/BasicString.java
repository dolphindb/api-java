package com.xxdb.data;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.temporal.Temporal;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB string scalar
 *
 */

public class BasicString extends AbstractScalar implements Comparable<BasicString>{
	private String value;
	private boolean isBlob = false;
	private byte[] blobValue;

	public BasicString(String value){
		this.value = value;
		isBlob = false;
	}

	public BasicString(String value,boolean blob){
		if (blob)
			blobValue = value.getBytes(StandardCharsets.UTF_8);
		else
			this.value = value;
		this.isBlob = blob;
	}

	public BasicString(byte[] value, boolean isBlob){
		if (isBlob){
			this.blobValue = value;
		}else {
			this.value = new String(value, StandardCharsets.UTF_8);
		}
		this.isBlob = isBlob;
	}

	public BasicString(ExtendedDataInput in) throws IOException{
		this.isBlob = false;
		value = in.readString();
	}

	public BasicString(ExtendedDataInput in, boolean blob) throws IOException{
		if (blob)
			this.blobValue = in.readBlob();
		else
			this.value = in.readString();
		this.isBlob = blob;
	}

	public String getString(){
		if (isBlob)
			return new String(blobValue, StandardCharsets.UTF_8);
		return value;
	}

	public byte[] getBytes(){
		if (isBlob)
			return blobValue;
		else
			throw new RuntimeException("The value must be a string scalar. ");

	}
	
	protected void setString(String value){
		if (isBlob)
			this.blobValue = value.getBytes(StandardCharsets.UTF_8);
		else
			this.value = value;
	}
	
	@Override
	public boolean isNull() {
		if (isBlob)
			return blobValue.length == 0;
		else
			return value.length() == 0;

	}
	
	@Override
	public void setNull() {
		if (isBlob)
			blobValue = new byte[0];
		else
			value = "";
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.LITERAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		if(this.isBlob == false)
			return Entity.DATA_TYPE.DT_STRING;
		else
			return Entity.DATA_TYPE.DT_BLOB;
	}
	
	public Number getNumber() throws Exception{
		throw new Exception("Imcompatible data type");
	}

	@Override
	public Temporal getTemporal() throws Exception {
		throw new Exception("Imcompatible data type");
	}
	
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicString) || o == null)
			return false;
		else{
			if (isBlob != ((BasicString)o).isBlob)
				return false;
			if (isBlob)
			{
				byte[] oValue = ((BasicString)o).blobValue;
				if (blobValue.length != oValue.length)
					return false;
				else
				{
					for(int i = 0; i < blobValue.length; ++i)
					{
						if (blobValue[i] != oValue[i])
							return false;
					}
				}
			}
			else
				return value.equals(((BasicString)o).value);
		}
		return true;
	}
	
	@Override
	public int hashCode(){
		if (isBlob)
		{
			StringBuilder stringBuilder = new StringBuilder();
			for(int i = 0; i < blobValue.length; ++i)
			{
				stringBuilder.append((char)blobValue[i]);
			}
			return stringBuilder.toString().hashCode();
		}

		return value.hashCode();
	}
	
	@Override
	public int hashBucket(int buckets){
		if (isBlob)
		{
			StringBuilder stringBuilder = new StringBuilder();
			for (int i = 0; i < blobValue.length; ++i)
			{
				stringBuilder.append((char)blobValue[i]);
			}
			return stringBuilder.toString().hashCode() % buckets;
		}
		else
			return hashBucket(this.value, buckets);
	}

	@Override
	public String getJsonString() {
		if(isNull()) return "null";
		return "\"" + getString() + "\"";
	}

	public static int hashBucket(String str, int buckets){
		return str.hashCode() % buckets;
	}
	
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		if (isBlob){
			writeScalarToOutputStream(out, true);
		}else {
			writeScalarToOutputStream(out, false);
		}
	}

	protected void writeScalarToOutputStream(ExtendedDataOutput out, boolean blob) throws IOException{
		if(!blob) {
			out.writeString(value);
		} else {
			out.writeBlob(blobValue);
		}

	}

	@Override
	public int compareTo(BasicString o) {
		if (isBlob)
			return new String(blobValue, StandardCharsets.UTF_8).compareTo(o.value);
		else
			return value.compareTo(o.value);

	}
}
