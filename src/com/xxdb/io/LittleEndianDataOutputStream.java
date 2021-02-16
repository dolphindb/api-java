package com.xxdb.io;

import java.io.IOException;
import java.io.OutputStream;

public class LittleEndianDataOutputStream extends AbstractExtendedDataOutputStream{

	public LittleEndianDataOutputStream(OutputStream out) {
		super(out);
	}
	@Override
	public void writeShort(int v) throws IOException {
		out.write(0xFF & v);
		out.write(0xFF & (v >> 8));
	}
	
	@Override
	public void writeInt(int v) throws IOException {
		out.write(0xFF & v);
		out.write(0xFF & (v >> 8));
		out.write(0xFF & (v >> 16));
		out.write(0xFF & (v >> 24));
	}

	@Override
	public void writeLong(long v) throws IOException {
		out.write((int)(0xFF & v));
		out.write((int)(0xFF & (v >> 8)));
		out.write((int)(0xFF & (v >> 16)));
		out.write((int)(0xFF & (v >> 24)));
		out.write((int)(0xFF & (v >> 32)));
		out.write((int)(0xFF & (v >> 40)));
		out.write((int)(0xFF & (v >> 48)));
		out.write((int)(0xFF & (v >> 56)));
	}
	
	public void writeLong2(Long2 v) throws IOException {
		writeLong(v.low);
		writeLong(v.high);
	}
	
	public void writeDouble2(Double2 v) throws IOException {
		writeDouble(v.x);
		writeDouble(v.y);
	}

	@Override
	public void writeIntArray(int [] A, int startIdx, int len) throws IOException{
		if (buf == null) {
			buf = new byte[BUF_SIZE];
		}
		int end = startIdx + len;
		int pos = 0;
		for (int i = startIdx; i < end; ++i) {
			int v = A[i];
			if (pos + 4 >= BUF_SIZE) {
				out.write(buf, 0, pos);
				pos = 0;
			}
			buf[pos++] = (byte)(0xFF & (v));
			buf[pos++] = (byte)(0xFF & (v >> 8));
			buf[pos++] = (byte)(0xFF & (v >> 16));
			buf[pos++] = (byte)(0xFF & (v >> 24));
		}
		if (pos > 0) {
			out.write(buf, 0, pos);
		}
	}

	@Override
	public void writeShortArray(short [] A, int startIdx, int len) throws IOException{
		if (buf == null) {
			buf = new byte[BUF_SIZE];
		}
		int end = startIdx + len;
		int pos = 0;
		for (int i = startIdx; i < end; ++i) {
			short v = A[i];
			if (pos + 2 >= BUF_SIZE) {
				out.write(buf, 0, pos);
				pos = 0;
			}
			buf[pos++] = (byte)(0xFF & (v));
			buf[pos++] = (byte)(0xFF & (v >> 8));
		}
		if (pos > 0) {
			out.write(buf, 0, pos);
		}
	}

	@Override
	public void writeLongArray(long[] A, int startIdx, int len) throws IOException {
		if (buf == null) {
			buf = new byte[BUF_SIZE];
		}
		int end = startIdx + len;
		int pos = 0;
		for (int i = startIdx; i < end; ++i) {
			long v = A[i];
			if (pos + 8 >= BUF_SIZE) {
				out.write(buf, 0, pos);
				pos = 0;
			}
			buf[pos++] = (byte)(0xFF & (v));
			buf[pos++] = (byte)(0xFF & (v >> 8));
			buf[pos++] = (byte)(0xFF & (v >> 16));
			buf[pos++] = (byte)(0xFF & (v >> 24));
			buf[pos++] = (byte)(0xFF & (v >> 32));
			buf[pos++] = (byte)(0xFF & (v >> 40));
			buf[pos++] = (byte)(0xFF & (v >> 48));
			buf[pos++] = (byte)(0xFF & (v >> 56));
		}
		if (pos > 0) {
			out.write(buf, 0, pos);
		}
	}
	
	@Override
	public void writeLong2Array(Long2[] A, int startIdx, int len) throws IOException {
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
			longBuf[pos++] = A[i].low;
			longBuf[pos++] = A[i].high;
		}
		if (pos > 0)
			writeLongArray(longBuf, 0, pos);
	}
}
