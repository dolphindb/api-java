package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Interface for DolphinDB data form: ARRAY, BIGARRAY
 *
 */

public interface Vector extends Entity{
	int DISPLAY_ROWS = 10;
	int COMPRESS_LZ4 = 1;
	int COMPRESS_DELTA = 2;

	Vector combine(Vector vector);
	Vector getSubVector(int[] indices);
	int asof(Scalar value);
	boolean isNull(int index);
	void setNull(int index);
	int hashBucket(int index, int buckets);
	Entity get(int index);
	String getString(int index);
	void set(int index, Entity value) throws Exception;
	Class<?> getElementClass();
	void deserialize(int start, int count, ExtendedDataInput in) throws IOException;
	void serialize(int start, int count, ExtendedDataOutput out) throws IOException;
	int getUnitLength();
	int serialize(int indexStart, int offect, int targetNumElement, AbstractVector.NumElementAndPartial numElementAndPartial, ByteBuffer out) throws IOException;
}
