package com.xxdb.io;

import java.io.DataOutput;
import java.io.IOException;

public interface ExtendedDataOutput extends DataOutput {
	void writeString(String str) throws IOException;
	void writeBlob(String v) throws IOException;
	void writeLong2(Long2 v) throws IOException;
	void writeDouble2(Double2 v) throws IOException;
	void flush() throws IOException;
	void writeShortArray(short[] A) throws IOException;
	void writeShortArray(short[] A, int startIdx, int len) throws IOException;
	void writeIntArray(int[] A) throws IOException;
	void writeIntArray(int[] A, int startIdx, int len) throws IOException;
	void writeLongArray(long[] A) throws IOException;
	void writeLongArray(long[] A, int startIdx, int len) throws IOException;
	void writeDoubleArray(double[] A) throws IOException;
	void writeDoubleArray(double[] A, int startIdx, int len) throws IOException;
	void writeFloatArray(float[] A) throws IOException;
	void writeFloatArray(float[] A, int startIdx, int len) throws IOException;
	void writeStringArray(String[] A) throws IOException;
	void writeStringArray(String[] A, int startIdx, int len) throws IOException;
	void writeLong2Array(Long2[] A) throws IOException;
	void writeLong2Array(Long2[] A, int startIdx, int len) throws IOException;
	void writeDouble2Array(Double2[] A) throws IOException;
	void writeDouble2Array(Double2[] A, int startIdx, int len) throws IOException;
}
