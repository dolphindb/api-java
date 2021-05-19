package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.xxdb.compression.EncoderFactory;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.LittleEndianDataOutputStream;

public abstract class AbstractVector extends AbstractEntity implements Vector{
	private DATA_FORM df_;
	protected static final int BUF_SIZE = 4096;
	protected byte[] buf = new byte[BUF_SIZE];
	protected int compressedMethod = Vector.COMPRESS_LZ4;
	
	protected abstract void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException;
	
	public AbstractVector(DATA_FORM df){
		df_ = df;
	}
	
	@Override
	public DATA_FORM getDataForm() {
		return df_;
	}
	
	@Override
	public int columns() {
		return 1;
	}
	
	@Override
	public int hashBucket(int index, int buckets){
		return -1;
	}
	
	@Override
	public String getString(int index){
		return get(index).getString();
	}
	
	public String getString(){
		StringBuilder sb = new StringBuilder("[");
		int size = Math.min(DISPLAY_ROWS, rows());
		if(size > 0)
			sb.append(get(0).getString());
		for(int i=1; i<size; ++i){
			sb.append(',');
			sb.append(get(i).getString());
		}
		if(size < rows())
			sb.append(",...");
		sb.append("]");
		return sb.toString();
	}
	
	public void write(ExtendedDataOutput out) throws IOException{
		int dataType = getDataType().ordinal();
		if(this instanceof BasicSymbolVector)
			dataType += 128;
		int flag = (df_.ordinal() << 8) + dataType;
		out.writeShort(flag);
		out.writeInt(rows());
		out.writeInt(columns());
		writeVectorToOutputStream(out);
	}

	public void setCompressedMethod(int method) {
		this.compressedMethod = method;
	}

	protected void writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		throw new RuntimeException("Invalid datatype to write to buffer");
	};

	@Override
	public void writeCompressed(ExtendedDataOutput output) throws IOException {
		int dataType = this.getDataType().ordinal();
		int unitLength;
		if (dataType >= 6 && dataType <= 11 || dataType == 4 || dataType == 15) {
			unitLength = 4;
		} else if (dataType >= 12 && dataType <= 14 || dataType == 5 || dataType == 16) {
			unitLength = 8;
		} else if (dataType == 3) {
			unitLength = 2;
		} else {
			throw new RuntimeException("Compression Failed: only support integral and temporal data, not support " + getDataType().name());
		}
		int elementCount = this.rows();
		int maxCompressedLength = this.rows() * Long.BYTES * 2;

		ByteBuffer out = output instanceof LittleEndianDataOutputStream ?
				ByteBuffer.allocate(maxCompressedLength).order(ByteOrder.LITTLE_ENDIAN) :
				ByteBuffer.allocate(maxCompressedLength).order(ByteOrder.BIG_ENDIAN);
		short flag = (short) (Entity.DATA_FORM.DF_VECTOR.ordinal() << 8 | Entity.DATA_TYPE.DT_COMPRESS.ordinal() & 0xff);

		out.putShort(flag);
		out.putInt(0);// compressedBytes
		out.putInt(1);// cols
		out.put((byte) 0); // version
		out.put((byte) 1); // flag bit0:littleEndian bit1:containChecksum
		out.put((byte) -1); // charcode
		out.put((byte) compressedMethod);
		out.put((byte) dataType);
		out.put((byte) unitLength);
		out.position(out.position() + 2); //reserved
		out.putInt(-1); //extra
		out.putInt(elementCount);
		out.putInt(-1); //TODO: checkSum

		ByteBuffer in = ByteBuffer.allocate(elementCount * unitLength);
		writeVectorToBuffer(in);
		in.flip();
		int compressedLength = 20 + EncoderFactory.get(compressedMethod).compress(in, elementCount, unitLength, maxCompressedLength, out);
		out.putInt(2, compressedLength);
		output.write(out.array(), 0, compressedLength);
	}
}
