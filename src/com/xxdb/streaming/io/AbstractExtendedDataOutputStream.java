package com.xxdb.streaming.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;

public abstract class AbstractExtendedDataOutputStream extends FilterOutputStream implements ExtendedDataOutput{
	private static final int UTF8_STRING_LIMIT = 65535;
	private byte[] buf;
	
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
		int len = s.length();
		for (int i = 0; i < len; ++i)
			writeByte(s.charAt(i));
	}

	@Override
	public void writeChars(String s) throws IOException {
		int len = s.length();
		for (int i = 0; i < len; ++i)
			writeChar(s.charAt(i));
	}

	@Override
	public void writeUTF(String value) throws IOException {
		int len = value.length();
		int i = 0;
		int pos = 0;
		boolean lengthWritten = false;
		if (buf == null)
			buf = new byte[512];
		do {
			while (i < len && pos < buf.length - 3){
				char c = value.charAt(i++);
				if (c >= '\u0001' && c <= '\u007f')
					buf[pos++] = (byte) c;
				else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff')){
					buf[pos++] = (byte) (0xc0 | (0x1f & (c >> 6)));
					buf[pos++] = (byte) (0x80 | (0x3f & c));
				}
				else{
					buf[pos++] = (byte) (0xe0 | (0x0f & (c >> 12)));
					buf[pos++] = (byte) (0x80 | (0x3f & (c >> 6)));
					buf[pos++] = (byte) (0x80 | (0x3f & c));
				}
			}
			if (! lengthWritten){
				if (i == len)
					writeShort(pos);
				else
					writeShort(getUTFlength(value, i, pos));
				lengthWritten = true;
			}
			write(buf, 0, pos);
			pos = 0;
		}while (i < len);
	}

	@Override
	public void writeString(String value) throws IOException {
		int len = value.length();
		int i = 0;
		int pos = 0;
		if (buf == null)
			buf = new byte[512];
		do {
			while (i < len && pos < buf.length - 4){
				char c = value.charAt(i++);
				if (c >= '\u0001' && c <= '\u007f')
					buf[pos++] = (byte) c;
				else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff')){
					buf[pos++] = (byte) (0xc0 | (0x1f & (c >> 6)));
					buf[pos++] = (byte) (0x80 | (0x3f & c));
				}
				else{
					buf[pos++] = (byte) (0xe0 | (0x0f & (c >> 12)));
					buf[pos++] = (byte) (0x80 | (0x3f & (c >> 6)));
					buf[pos++] = (byte) (0x80 | (0x3f & c));
				}
			}
			if(i >= len)
				buf[pos++] = 0;
			write(buf, 0, pos);
			pos = 0;
		}while (i < len);
	}

	private int getUTFlength(String value, int start, int sum) throws IOException {
		int len = value.length();
		for (int i = start; i < len && sum <= 65535; ++i){
			char c = value.charAt(i);
			if (c >= '\u0001' && c <= '\u007f')
				sum += 1;
			else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff'))
				sum += 2;
			else
				sum += 3;
		}

		if (sum > UTF8_STRING_LIMIT)
			throw new UTFDataFormatException ();
		return sum;
	}
}
