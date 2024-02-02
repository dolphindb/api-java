package com.xxdb.io;

import java.io.*;
import java.nio.charset.Charset;

public abstract class AbstractExtendedDataInputStream extends BufferedInputStream implements ExtendedDataInput{
	private static final Charset UTF8 = Charset.forName("UTF-8");
	private static final int UTF8_STRING_LIMIT = 65536;
	private byte[] buf_;

	protected AbstractExtendedDataInputStream(InputStream in) {
		super(in, 8192);
	}

    @Override
	public boolean readBoolean() throws IOException {
		return readUnsignedByte() != 0;
	}

	@Override
	public byte readByte() throws IOException {
		return (byte)readUnsignedByte();
	}

	@Override
	public char readChar() throws IOException {
		return (char)readUnsignedShort();
	}

	@Override
	public double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
	}

	@Override
	public float readFloat() throws IOException {
		return Float.intBitsToFloat(readInt());
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		int read;
		do {
			read = super.read(b, off, len);
			len -= read;
			off += read;
		} while (len > 0);
	}
	
	@Override
	public String readLine() throws IOException {
		return readUTF8((byte)'\n');
	}
	
	@Override
	public String readString() throws IOException {
		return readUTF8((byte)0);
	}

	private int isHaveEndBytes(byte endChar){
		byte[] streamBuf = this.buf;
		for (int i = this.pos; i < this.count; ++i)
			if(streamBuf[i] == endChar) return i;

		return -1;
	}

	private InputStream getInIfOpen() throws IOException {
		InputStream input = in;
		if (input == null)
			throw new IOException("Stream closed");
		return input;
	}

	private void fill() throws IOException {
		byte[] buffer = buf;
		pos = 0;
		count = 0;
		int n = getInIfOpen().read(buffer, pos, buffer.length - pos);
		if(n == -1)
			throw new EOFException();
		count = n;
	}

	private String readUTF8(byte terminator) throws IOException{
		if(buf_ == null)
			buf_ = new byte[2048];
		int bufPos = 0;
		while(true){
			int terminatorPos = isHaveEndBytes(terminator);
			int dataCount = terminatorPos == -1 ? this.count - this.pos : terminatorPos - this.pos + 1;
			if(dataCount + bufPos > buf_.length){
				int bufferSize = Math.max(dataCount + bufPos, buf_.length * 2);
				byte[] tmp = new byte[bufferSize];
				System.arraycopy(buf_, 0, tmp, 0, buf_.length);
				buf_ = tmp;
			}
			System.arraycopy(buf, pos, buf_, bufPos, dataCount);
			bufPos += dataCount;
			this.pos += dataCount;
			if(terminatorPos != -1) {
				bufPos--;
				break;
			}
			fill();
		}
		return new String(buf_, 0, bufPos, UTF8);
	}
	
	@Override
	public byte[] readBlob() throws IOException{
		int len = readInt();
		int offset = 0;
		int actualSize = 0;
		byte[] buff = new byte[len];
		while (offset < len){
			actualSize = read(buff, offset, len-offset);
			offset += actualSize;
		}
		return buff;
	}

	@Override
	public short readShort() throws IOException {
		return (short)readUnsignedShort();
	}

	@Override
	public String readUTF() throws IOException {
		return new DataInputStream(in).readUTF();
	}
	
	@Override
	public int readUnsignedByte() throws IOException {

		int b1 = read();
		if (0 > b1) {
		    throw new EOFException();
		}
		return b1;
	}
	
	@Override
	public int skipBytes(int n) throws IOException {
		int actualSkip = 0;
		int reamainSkip = n - actualSkip;
		while (reamainSkip > 0){
			actualSkip = (int) skip(reamainSkip);
			reamainSkip = reamainSkip - actualSkip;
		}
		return actualSkip;
	}
	
	protected byte readAndCheckByte() throws IOException, EOFException {
		int b1 = read();
		
		if (-1 == b1) {
		    throw new EOFException();
		}
		return (byte) b1;
	}
	
	protected int fromBytes(byte b1, byte b2, byte b3, byte b4){
		return b1 << 24 | (b2 & 0xFF) << 16 | (b3 & 0xFF) << 8 | (b4 & 0xFF);
	}
	
	protected long fromBytes(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7, byte b8) {
		return (b1 & 0xFFL) << 56 | (b2 & 0xFFL) << 48 | (b3 & 0xFFL) << 40 | (b4 & 0xFFL) << 32
			 | (b5 & 0xFFL) << 24 | (b6 & 0xFFL) << 16 | (b7 & 0xFFL) << 8  | (b8 & 0xFFL);
	}

}
