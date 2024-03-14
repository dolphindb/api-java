package com.xxdb.data;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.temporal.Temporal;
import com.fasterxml.jackson.annotation.JsonIgnore;
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
		if (isNull())
			return "";
		return value;
	}

	@JsonIgnore
	public byte[] getBytes(){
		if (isBlob)
			return blobValue;
		else
			throw new RuntimeException("The value must be a string scalar. ");
	}

	public byte[] getBlobValue() {
		return blobValue;
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

	@JsonIgnore
	public Number getNumber() throws Exception{
		throw new Exception("Imcompatible data type");
	}

	@JsonIgnore
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
			return hashBucket(stringBuilder.toString(), buckets);
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
		int length = str.length();

		//check utf8 bytes
		int bytes = 0;
		for(int i=0; i<length; ++i){
			char c = str.charAt(i);
			if (c >= '\u0001' && c <= '\u007f')
				++bytes;
			else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff'))
				bytes += 2;
			else
				bytes += 3;
		}

		//calculate murmur32 hash
		int h =bytes;
		if(bytes == length){
			int length4 = bytes / 4;
			for (int i = 0; i < length4; i++) {
				final int i4 = i * 4;
				int k = (str.charAt(i4) & 0xff) + ((str.charAt(i4+1) & 0xff) << 8)
						+ ((str.charAt(i4+2) & 0xff) << 16) + ((str.charAt(i4+3) & 0xff) << 24);
				k *= 0x5bd1e995;
				k ^= k >>> 24;
				k *= 0x5bd1e995;
				h *= 0x5bd1e995;
				h ^= k;
			}
			// Handle the last few bytes of the input array
			switch (bytes % 4) {
				case 3:
					h ^= (str.charAt((bytes & ~3) + 2) & 0xff) << 16;
				case 2:
					h ^= (str.charAt((bytes & ~3) + 1) & 0xff) << 8;
				case 1:
					h ^= str.charAt(bytes & ~3) & 0xff;
					h *= 0x5bd1e995;
			}

			h ^= h >>> 13;
			h *= 0x5bd1e995;
			h ^= h >>> 15;
		}
		else{
			int k=0;
			int cursor = 0;
			for (int i=0; i<length; ++i){
				char c = str.charAt(i);
				if (c >= '\u0001' && c <= '\u007f'){
					k += c << (8*cursor++);
				}
				else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff')){
					k +=  (0xc0 | (0x1f & (c >> 6))) << (8*cursor++);
					if(cursor == 4){
						k *= 0x5bd1e995;
						k ^= k >>> 24;
						k *= 0x5bd1e995;
						h *= 0x5bd1e995;
						h ^= k;
						k = 0;
						cursor = 0;
					}
					k +=  (0x80 | (0x3f & c)) << (8*cursor++);
				}
				else{
					k +=  (0xe0 | (0x0f & (c >> 12))) << (8*cursor++);
					if(cursor == 4){
						k *= 0x5bd1e995;
						k ^= k >>> 24;
						k *= 0x5bd1e995;
						h *= 0x5bd1e995;
						h ^= k;
						k = 0;
						cursor = 0;
					}
					k +=  (0x80 | (0x3f & (c >> 6))) << (8*cursor++);
					if(cursor == 4){
						k *= 0x5bd1e995;
						k ^= k >>> 24;
						k *= 0x5bd1e995;
						h *= 0x5bd1e995;
						h ^= k;
						k = 0;
						cursor = 0;
					}
					k +=  (0x80 | (0x3f & c)) << (8*cursor++);
				}
				if(cursor == 4){
					k *= 0x5bd1e995;
					k ^= k >>> 24;
					k *= 0x5bd1e995;
					h *= 0x5bd1e995;
					h ^= k;
					k = 0;
					cursor = 0;
				}
			}
			if(cursor > 0){
				h ^= k;
				h *= 0x5bd1e995;
			}

			h ^= h >>> 13;
			h *= 0x5bd1e995;
			h ^= h >>> 15;
		}
		if(h>=0)
			return h % buckets;
		else{
			return (int)((4294967296l + h) % buckets);
		}
	}
	
	public void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
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

	@JsonIgnore
	@Override
	public int getScale() {
		return super.getScale();
	}
}
