package com.xxdb.compression;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * BasicTable -> BasicTable / vector -> vector
 * input: datainputstream, output: long[]
 */
public class DeltaOfDeltaEncoder extends AbstractEncoder {
    private static final int DEFAULT_BLOCK_SIZE = 65536;

    @Override
    public int compress(ByteBuffer in, int elementCount, int unitLength, int maxCompressedLength, ByteBuffer out) throws IOException {
        DeltaOfDeltaBlockEncoder blockEncoder = new DeltaOfDeltaBlockEncoder(unitLength);
        int count = 0;
        while (elementCount > 0) {
            int blockSize = Math.min(elementCount * unitLength, DEFAULT_BLOCK_SIZE);
            long[] compressed = blockEncoder.compress(in, blockSize);
            //write blockSize+data
            out.putInt(compressed.length * Long.BYTES);
            for (long l : compressed) {
                out.putLong(l);
            }
            count += Integer.BYTES + compressed.length * Long.BYTES;
            elementCount -= blockSize / unitLength;
        }
        return count;
    }

    public ByteBuffer compress(ByteBuffer in, int elementCount, int unitLength, int maxCompressedLength) throws IOException {
        DeltaOfDeltaBlockEncoder blockEncoder = new DeltaOfDeltaBlockEncoder(unitLength);
        int count = 0;
        //TODO: create header in advanced
        ByteBuffer out = ByteBuffer.allocate(maxCompressedLength);
        while (elementCount > 0 && count < maxCompressedLength) {
            int blockSize = Math.min(elementCount * unitLength, DEFAULT_BLOCK_SIZE);
            long[] compressed = blockEncoder.compress(in, blockSize);
            //write blockSize+data
            out.putInt(compressed.length * Long.BYTES);
            for (long l : compressed) {
                out.putLong(l);
            }
            count += Integer.BYTES + compressed.length * Long.BYTES;
            elementCount -= blockSize / unitLength;
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
//  TODO: if(src.read() == null) writeBit(0,1);
//        while (count < dataSize) {
//            count++;
//        }
        writeHeader();
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

    private boolean writeNull() {
        out.writeBits(63L, 6);
        return true;
    }

    private void writeNext() {
        //TODO: NULL VALUE
        // if(null) -> writeNull;
        long value = readBuffer(in);
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
        count--;
        throw new RuntimeException("Compression fails: can only support integral or temporal type");
    }


}
