package com.xxdb.compression;

import com.xxdb.data.AbstractVector;
import com.xxdb.data.BasicStringVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.xxdb.data.Utils.reAllocByteBuffer;

/**
 * input: datainputstream, output: long[]
 */
public class DeltaOfDeltaEncoder implements Encoder {
    private static final int DEFAULT_BLOCK_SIZE = 65536;

    @Override
    public ByteBuffer compress(AbstractVector in, int elementCount, int unitLength, int maxCompressedLength, ByteBuffer out) throws IOException {
        DeltaOfDeltaBlockEncoder blockEncoder = new DeltaOfDeltaBlockEncoder(unitLength);
        int count = 0;
        int dataCount = in.rows();
        int dataIndex = 0;
        ByteBuffer dataBufer = ByteBuffer.allocate(DEFAULT_BLOCK_SIZE);
        boolean isLittleEndian = out.order() == ByteOrder.LITTLE_ENDIAN;
        if (isLittleEndian)
            dataBufer.order(ByteOrder.LITTLE_ENDIAN);
        else
            dataBufer.order(ByteOrder.BIG_ENDIAN);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(0, 0);
        while (dataCount > dataIndex)
        {
            int targetNum = Math.min(dataCount-dataIndex, (DEFAULT_BLOCK_SIZE) / unitLength);
            int readBytes = in.serialize(dataIndex, numElementAndPartial.partial, targetNum, numElementAndPartial, dataBufer);
            dataIndex += numElementAndPartial.numElement;
            while (readBytes > 0)
            {
                int blockSize = Math.min(readBytes, DEFAULT_BLOCK_SIZE);
                long[] compressed = blockEncoder.compress(dataBufer, blockSize);
                //write blockSize+data
                out.putInt(compressed.length * Long.BYTES);
                for (long l : compressed){
                    if (out.remaining() < Long.BYTES)
                        out = reAllocByteBuffer(out, out.capacity() * 2);
                    out.putLong(l);
                }
                count += Integer.BYTES + compressed.length * Long.BYTES;
                readBytes -= blockSize;
                dataBufer.clear();
            }
        }
        return out;
    }
}

class DeltaOfDeltaBlockEncoder {
    private final int unitLength;
    private final int FIRST_DELTA_BITS;

    private ByteBuffer in;
    private DeltaBitOutput out;
    private long storedValue;
    private long storedDelta;
    private int count;

    DeltaOfDeltaBlockEncoder(int unitLength) {
        this.unitLength = unitLength;
        this.FIRST_DELTA_BITS = unitLength * Byte.SIZE;
    }

    public static long encodeZigZag64(final long n) {
        return (n << 1) ^ (n >> 63);
    }

    public long[] compress(ByteBuffer src, int blockSize) {
        this.out = new DeltaBitOutput();
        this.in = src;
        this.count = 0;
        long value = 0;
        in.flip();
        while(count * unitLength < blockSize){
            value = readBuffer(in);
            if(!(unitLength == 2 && value == Short.MIN_VALUE || unitLength == 4 && value == Integer.MIN_VALUE || unitLength == 8 && value == Long.MIN_VALUE))
                break;
            out.writeBits(0, 1);
        }
        count--;
        if(count * unitLength >= blockSize){
            writeEnd();
            return this.out.getLongArray();
        }
        in.position(in.position() - unitLength);
        writeHeader();
        while(count * unitLength < blockSize){
            value = readBuffer(in);
            if(!(unitLength == 2 && value == -32768 || unitLength == 4 && value == -2147483648 || unitLength == 8 && value == -9223372036854775808L))
                break;
            out.writeBits(0, 1);
        }
        count--;
        if(count * unitLength >= blockSize){
            writeEnd();
            return this.out.getLongArray();
        }
        in.position(in.position() - unitLength);
        writeFirstDelta();
        while (count * unitLength < blockSize) {
            try {
                writeNext();
            } catch (Exception ex) {
                break;
            }
        }
        writeEnd();
        return this.out.getLongArray();
    }

    private void writeHeader() {
        storedValue = readBuffer(in);
        out.writeBit();
        out.writeBits(encodeZigZag64(storedValue), unitLength * Byte.SIZE);
    }

    private void writeFirstDelta() {
        out.writeBit();
        long value = readBuffer(in);
        storedDelta = value - storedValue;
        out.writeBits(encodeZigZag64(storedDelta), FIRST_DELTA_BITS);
        storedValue = value;
    }

    private void writeNext() {
        //TODO: NULL VALUE
        // if(null) -> writeNull;
        long value = readBuffer(in);
        if(unitLength == 2 && value == Short.MIN_VALUE || unitLength == 4 && value == Integer.MIN_VALUE || unitLength == 8 && value == Long.MIN_VALUE){
            compressDataNull();
            return;
        }
        long newDelta = Math.subtractExact(value, storedValue);
        long deltaD = Math.subtractExact(newDelta, storedDelta);
        //FIXME: implement based on the c++ code
        if (deltaD == 0) {
            out.skipBit();
        } else {
            deltaD = encodeZigZag64(deltaD);
            deltaD--;
            if (deltaD < 1L << 7) {
                out.writeBits(2L, 2); // store '10'
                out.writeBits(deltaD, 7); // Using 7 bits, store the value..
            } else if (deltaD < 1L << 9) {
                out.writeBits(6L, 3); // store '110'
                out.writeBits(deltaD, 9); // Use 9 bits
            } else if (deltaD < 1L << 16) {
                out.writeBits(14L, 4); // store '1110'
                out.writeBits(deltaD, 16); // Use 12 bits
            } else if (deltaD < 1L << 32) {
                out.writeBits(30L, 5); // Store '11110'
                out.writeBits(deltaD, 32); // Store delta using 32 bits
            } else {
                out.writeBits(62L, 6); // Store '111110'
                out.writeBits(deltaD, 64); // Store delta using 64 bits
            }
        }
        storedDelta = newDelta;
        storedValue = value;
    }

    public void compressDataNull() {
        out.writeBits(63, 6);
    }

    public void writeEnd() {
        out.writeBits(62, 6);
        out.writeBits(0xFFFFFFFFFFFFFFFFL, 64);
        out.skipBit();
    }

    private long readBuffer(ByteBuffer in) {
        count++;
        if (unitLength == 2)
            return in.getShort();
        else if (unitLength == 4)
            return in.getInt();
        else if (unitLength == 8)
            return in.getLong();
        throw new RuntimeException("Compression fails: can only support integral or temporal type");
    }


}
