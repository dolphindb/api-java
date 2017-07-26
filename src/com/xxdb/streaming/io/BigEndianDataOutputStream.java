package com.xxdb.streaming.io;

import java.io.IOException;
import java.io.OutputStream;

public class BigEndianDataOutputStream extends AbstractExtendedDataOutputStream{

	public BigEndianDataOutputStream(OutputStream out) {
		super(out);
	}
	
	@Override
	public void writeShort(int v) throws IOException {
		write (0xff & (v >> 8));
		write (0xff & v);
	}
	
	@Override
	public void writeInt(int v) throws IOException {
		out.write(0xFF & (v >> 24));
		out.write(0xFF & (v >> 16));
		out.write(0xFF & (v >> 8));
		out.write(0xFF & v);
	}

	@Override
	public void writeLong(long v) throws IOException {
		out.write((int)(0xFF & (v >> 56)));
		out.write((int)(0xFF & (v >> 48)));
		out.write((int)(0xFF & (v >> 40)));
		out.write((int)(0xFF & (v >> 32)));
		out.write((int)(0xFF & (v >> 24)));
		out.write((int)(0xFF & (v >> 16)));
		out.write((int)(0xFF & (v >> 8)));
		out.write((int)(0xFF & v));
	}
}
