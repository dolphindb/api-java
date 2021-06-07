package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

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
		DATA_TYPE type = this.getDataType();
		switch (type){
			case DT_STRING:
			case DT_BOOL:
			case DT_BYTE:
			case DT_SHORT:
			case DT_INT:
			case DT_DATE:
			case DT_MONTH:
			case DT_TIME:
			case DT_MINUTE:
			case DT_SECOND:
			case DT_DATETIME:
			case DT_DATEHOUR:
			case DT_DATEMINUTE:
			case DT_SYMBOL:
			case DT_LONG:
			case DT_NANOTIME:
			case DT_TIMESTAMP:
			case DT_NANOTIMESTAMP:
			case DT_INT128:
			case DT_UUID:
			case DT_IPADDR:
			case DT_POINT:
				break;
			case DT_FLOAT:
			case DT_DOUBLE:
			case DT_COMPLEX:
				if(method == Vector.COMPRESS_LZ4)
					break;
			default:
				throw new RuntimeException("Compression Failed: only support integral and temporal data, not support " + getDataType().name());
		}
		this.compressedMethod = method;
	}

	protected ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		throw new RuntimeException("Invalid datatype to write to buffer");
	};

	public static int getUnitLength(Entity.DATA_TYPE type) {
		int unitLength = 0;
		switch (type) {
			case DT_STRING:
				unitLength = 0;
				break;
			case DT_BOOL:
			case DT_BYTE:
				unitLength = 1;
				break;
			case DT_SHORT:
				unitLength = 2;
				break;
			case DT_INT:
			case DT_DATE:
			case DT_MONTH:
			case DT_TIME:
			case DT_MINUTE:
			case DT_SECOND:
			case DT_DATETIME:
			case DT_FLOAT:
			case DT_DATEHOUR:
			case DT_DATEMINUTE:
			case DT_SYMBOL:
				unitLength = 4;
				break;
			case DT_LONG:
			case DT_DOUBLE:
			case DT_NANOTIME:
			case DT_TIMESTAMP:
			case DT_NANOTIMESTAMP:
				unitLength = 8;
				break;
			case DT_INT128:
			case DT_UUID:
			case DT_IPADDR:
			case DT_COMPLEX:
			case DT_POINT:
				unitLength = 16;
				break;
			default:
				throw new RuntimeException("Compression Failed: only support integral and temporal data, not support " + type.name());
		}
		return unitLength;
	}

	@Override
	public void writeCompressed(ExtendedDataOutput output) throws IOException {
		int dataType = this.getDataType().ordinal();
		int unitLength = getUnitLength(this.getDataType());

		int elementCount = this.rows();
		int maxCompressedLength = this.rows() * Long.BYTES * 2 + 64 * 3;

		ByteBuffer out = output instanceof LittleEndianDataOutputStream ?
				ByteBuffer.allocate(Math.max(elementCount * unitLength, 655360)).order(ByteOrder.LITTLE_ENDIAN) :
				ByteBuffer.allocate(Math.max(elementCount * unitLength, 655360)).order(ByteOrder.BIG_ENDIAN);
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

		ByteBuffer in = output instanceof LittleEndianDataOutputStream ?
				ByteBuffer.allocate(Math.max(elementCount * unitLength, 655360)).order(ByteOrder.LITTLE_ENDIAN) :
				ByteBuffer.allocate(Math.max(elementCount * unitLength, 655360)).order(ByteOrder.BIG_ENDIAN);
		in = writeVectorToBuffer(in);
		in.flip();
		List<Object> ret = EncoderFactory.get(compressedMethod).compress(in, elementCount, unitLength, maxCompressedLength, out);
		int compressedLength = 20 + (int)ret.get(0);
		out = (ByteBuffer)ret.get(1);
		out.putInt(2, compressedLength);
		output.write(out.array(), 0, compressedLength + 10);
	}
}
