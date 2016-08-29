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
}
