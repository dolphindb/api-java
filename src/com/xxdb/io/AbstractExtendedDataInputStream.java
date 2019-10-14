package com.xxdb.io;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public abstract class AbstractExtendedDataInputStream extends FilterInputStream implements ExtendedDataInput{
	private static final Charset UTF8 = Charset.forName("UTF-8");
	private static final int UTF8_STRING_LIMIT = 65536;
	private byte[] buf_;

	protected AbstractExtendedDataInputStream(InputStream in) {
		super(in);
	}

    @Override
    public int available() throws IOException {
	    int re ;
	    try {
            for (int i=0;i<50;i++) {
                re = super.available();
                if (re > 0)
                    return re;
                Thread.sleep(10);
            }
        }catch(InterruptedException iex){
	        iex.printStackTrace();
        }
        return 0;
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
	
	private String readUTF8(byte terminator) throws IOException{
		if(buf_ == null)
			buf_ = new byte[2048];
		byte ch = readAndCheckByte();
		int count = 0;
		while(ch != terminator){
			if(count == buf_.length){
				if(count >= UTF8_STRING_LIMIT)
					throw new IOException("UTF-8 string length exceeds the limit of 65536 bytes");
				byte[] tmp = new byte[buf_.length*2];
				System.arraycopy(buf_, 0, tmp, 0, buf_.length);
				buf_ = tmp;
			}
			buf_[count++] = ch;
			ch = readAndCheckByte();
		}
		return new String(buf_, 0, count, UTF8);
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

		int b1 = in.read();
		if (0 > b1) {
		    throw new EOFException();
		}
		return b1;
	}
	
	@Override
	public int skipBytes(int n) throws IOException {
		return (int) in.skip(n);
	}
	
	protected byte readAndCheckByte() throws IOException, EOFException {
		int b1 = in.read();
		
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
