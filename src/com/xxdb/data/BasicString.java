package com.xxdb.data;

import java.io.IOException;
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

	public BasicString(String value){
		this.value = value;
	}

	public BasicString(ExtendedDataInput in) throws IOException{
		value = in.readString();
	}

	public BasicString(ExtendedDataInput in, boolean blob) throws IOException{
		if(!blob)
			value = in.readString();
		else
			value = in.readBlob();
	}

	public String getString(){
		return value;
	}
	
	protected void setString(String value){
		this.value = value;
	}
	
	@Override
	public boolean isNull() {
		return  value.isEmpty();
	}
	
	@Override
	public void setNull() {
		value = "";
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.LITERAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_STRING;
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
		else
			return value.equals(((BasicString)o).value);
	}
	
	@Override
	public int hashCode(){
		return value.hashCode();
	}
	
	@Override
	public int hashBucket(int buckets){
		return hashBucket(value, buckets);
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
	
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeString(value);
	}

	protected void writeScalarToOutputStream(ExtendedDataOutput out, boolean blob) throws IOException{
		if(!blob) {
			out.writeString(value);
		} else {
			out.writeBlob(value);
		}

	}

	@Override
	public int compareTo(BasicString o) {
		return value.compareTo(o.value);
	}
}
