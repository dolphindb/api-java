package com.xxdb.io;


import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public abstract class AbstractExtendedDataOutputStream extends FilterOutputStream implements ExtendedDataOutput{
	private static final int UTF8_STRING_LIMIT = 65535;
	protected static final int BUF_SIZE = 4096;
	protected byte[] buf;
	protected static final int longBufSize = BUF_SIZE / 8;
	protected static final int intBufSize = BUF_SIZE / 4;
	protected static final int doubleBufSize = BUF_SIZE / 8;
	protected int[] intBuf;
	protected long[] longBuf;
	protected double[] doubleBuf;
	private static final String SYMBOL_LENGTH_EXCEED_MSG = "Serialized symbol length must be less than 256k bytes.";
	private static final String STRING_LENGTH_EXCEED_MSG = "Serialized string length must be less than 64k bytes.";
	private static final int SERIALIZED_SYMBOL_MAX_LENGTH = 262144;
	private static final int SERIALIZED_STRING_MAX_LENGTH = 65536;

	public AbstractExtendedDataOutputStream(OutputStream out) {
		super(out);
	}
	
	public void flush() throws IOException{
		out.flush();
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		write(v?1:0);
	}

	@Override
	public void writeByte(int v) throws IOException {
		write(v & 0xff);
	}

	@Override
	public void writeChar(int v) throws IOException {
		writeShort(v);
	}

	@Override
	public void writeFloat(float v) throws IOException {
		 writeInt(Float.floatToIntBits (v));
	}

	@Override
	public void writeDouble(double v) throws IOException {
		writeLong(Double.doubleToLongBits (v));
	}

	@Override
	public void writeBytes(String s) throws IOException {
		byte[] b = s.getBytes(StandardCharsets.UTF_8);
		write(b);
	}

	@Override
	public void writeChars(String s) throws IOException {
		int len = s.length();
		for (int i = 0; i < len; ++i)
			writeChar(s.charAt(i));
	}

	@Override
	public void writeUTF(String value) throws IOException {
		byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
		byte[] newValueBytes = getResolveZeroByteArray(valueBytes);

		writeShort(newValueBytes.length);
		write(newValueBytes, 0, newValueBytes.length);
		write(0);
	}

	@Override
	public void writeString(String value, boolean isSymbol) throws IOException{
		int utf8Length = value.getBytes(StandardCharsets.UTF_8).length;
		if ((isSymbol && utf8Length >= SERIALIZED_SYMBOL_MAX_LENGTH) || (!isSymbol && utf8Length >= SERIALIZED_STRING_MAX_LENGTH))
			throw new RuntimeException(isSymbol ? SYMBOL_LENGTH_EXCEED_MSG : STRING_LENGTH_EXCEED_MSG);

		byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
		byte[] newValueBytes = getResolveZeroByteArray(valueBytes);
		write(newValueBytes, 0, newValueBytes.length);
		write(0);
	}

	@Override
	public void writeBlob(byte[] value) throws IOException {
		writeInt(value.length);
		write(value);
	}

	public static int getUTFlength(String value, int start, int sum) throws IOException {
		int len = value.length();
		for (int i = start; i < len; ++i){
			char c = value.charAt(i);
			if (c >= '\u0001' && c <= '\u007f')
				sum += 1;
			else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff'))
				sum += 2;
			else
				sum += 3;
		}
		return sum;
	}
	
	@Override
	public void writeShortArray(short[] A) throws IOException {
		writeShortArray(A, 0, A.length);
	}
	
	@Override
	public void writeIntArray(int[] A) throws IOException {
		writeIntArray(A, 0, A.length);
	}
	
	@Override
	public void writeLongArray(long[] A) throws IOException {
		writeLongArray(A, 0, A.length);
	}
	
	@Override
	public void writeDoubleArray(double[] A) throws IOException {
		writeDoubleArray(A, 0, A.length);
	}

	@Override
	public void writeDoubleArray(double[] A, int startIdx, int len) throws IOException {
		if (longBuf == null) {
			longBuf = new long[longBufSize];
		}
		int end = startIdx + len;
		int pos = 0;
		for (int i = startIdx; i < end; ++i) {
			if (pos >= longBufSize) {
				writeLongArray(longBuf,0, pos);
				pos = 0;
			}
			longBuf[pos++] = Double.doubleToLongBits(A[i]);
		}
		if (pos > 0)
			writeLongArray(longBuf, 0, pos);
	}

	@Override
	public void writeFloatArray(float[] A) throws IOException {
		writeFloatArray(A, 0, A.length);
	}
	@Override
	public void writeFloatArray(float[] A, int startIdx, int len) throws IOException {
		if (intBuf == null) {
			intBuf = new int[intBufSize];
		}
		int end = startIdx + len;
		int pos = 0;
		for (int i = startIdx; i < end; ++i) {
			if (pos >= intBufSize) {
				writeIntArray(intBuf,0, pos);
				pos = 0;
			}
			intBuf[pos++] = Float.floatToIntBits(A[i]);
		}
		if (pos > 0)
			writeIntArray(intBuf,0, pos);
	}
	@Override
	public void writeStringArray(String[] A) throws IOException {
		writeStringArray(A, 0, A.length);
	}

	@Override
	public void writeStringArray(String[] A, int startIdx, int len) throws IOException {
		int end = startIdx + len;
		for (int j = startIdx; j < end; ++j) {
			String value = A[j];
			byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
			byte[] newValueBytes = getResolveZeroByteArray(valueBytes);
			write(newValueBytes, 0, newValueBytes.length);
			write(0);
		}
	}
	
	@Override
	public void writeLong2Array(Long2[] A) throws IOException {
		writeLong2Array(A, 0, A.length);
	}
	
	@Override
	public void writeDouble2Array(Double2[] A, int startIdx, int len) throws IOException {
		if (doubleBuf == null) {
			doubleBuf = new double[doubleBufSize];
		}
		int end = startIdx + len;
		int pos = 0;
		for (int i = startIdx; i < end; ++i) {
			if (pos >= doubleBufSize) {
				writeDoubleArray(doubleBuf,0, pos);
				pos = 0;
			}
			doubleBuf[pos++] = A[i].x;
			doubleBuf[pos++] = A[i].y;
		}
		if (pos > 0)
			writeDoubleArray(doubleBuf, 0, pos);
	}
	
	@Override
	public void writeDouble2Array(Double2[] A) throws IOException {
		writeDouble2Array(A, 0, A.length);
	}

	public static byte[] getResolveZeroByteArray(byte[] originalArray) {

		int indexOfZero = -1;

		// Find the index of the first occurrence of 0.
		for (int i = 0; i < originalArray.length; i++) {
			if (originalArray[i] == 0) {
				indexOfZero = i;
				break;
			}
		}

		byte[] newArray;
		if (indexOfZero != -1) {
			// Create a new array containing only the elements before 0.
			System.out.println("The input String contains '\0', it will be dropped start from '\0' to last.");
			newArray = new byte[indexOfZero];
			System.arraycopy(originalArray, 0, newArray, 0, indexOfZero);
		} else {
			// If 0 is not found, the new array is the same as the original array.
			newArray = originalArray;
		}

		return newArray;
	}
	
}
