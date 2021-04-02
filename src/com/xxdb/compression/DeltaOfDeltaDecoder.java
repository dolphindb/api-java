package com.xxdb.compression;

import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.ExtendedDataInput;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DeltaOfDeltaDecoder extends AbstractDecoder {

    @Override
    public ExtendedDataInput decompress(DataInput in, int length, int unitLength, int elementCount, boolean isLittleEndian) throws IOException{
    	//TODO: handle the case of unitLength == 0 (String)
      	int offset = 8;
      	ByteBuffer dest = createColumnVector(elementCount, unitLength, false);
    	byte[] out = dest.array();
        int outLength = out.length - offset;
        int count = 0;
        DeltaOfDeltaBlockDecoder blockDecoder = new DeltaOfDeltaBlockDecoder(unitLength);
        
        while (length > 0 && count < outLength) {
            int blockSize = in.readInt();
            if(blockSize < 0){
            	blockSize = blockSize & 2147483647;
            }
            length -= Integer.BYTES;
            blockSize = Math.min(blockSize, length);
            if (blockSize == 0) break;
            System.out.println(blockSize);
            long[] src = new long[blockSize / Long.BYTES];
            for (int i = 0; i < src.length; i++) {
                src[i] = in.readLong();
            }
            
            count += blockDecoder.decompress(src, dest) * unitLength;
            length -= blockSize;
        }
        return new BigEndianDataInputStream(new ByteArrayInputStream(out, 0, offset + count));
    }
}

class DeltaOfDeltaBlockDecoder {
    private final int unitLength;
    private final int FIRST_DELTA_BITS;

    private DeltaBitInput in;
    private ByteBuffer dest;
    private long storedValue;
    private long storedDelta;

    public DeltaOfDeltaBlockDecoder(int unitLength) {
        this.unitLength = unitLength;
        this.FIRST_DELTA_BITS = unitLength * Byte.SIZE;
    }

    public static long decodeZigZag64(final long n) {
        return (n >>> 1) ^ -(n & 1);
    }

    public int decompress(long[] src, ByteBuffer dest) {
        this.dest = dest;
        this.storedValue = 0;
        this.storedDelta = 0;
        int count = 0;
        in = new DeltaBitInput(src);
        boolean flag = in.readBit();
        while (!flag) {
            flag = in.readBit();
            writeBuffer(this.dest, 0L);
            count++;
            if (in.getPosition() > src.length - 3) return count;
        }
        if (!readHeader())
            return count;
        count++;
        if (!first())
            return count;
        count++;
        while (in.getPosition() < src.length) {
            if (!nextValue())
                return count;
            count++;
        }
        return count;
    }

    private boolean readHeader() {
        try {
            storedValue = decodeZigZag64(in.getLong(unitLength * 8));
            System.out.println("header: " + storedValue);
            writeBuffer(dest, storedValue);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean first() {
        try {
            boolean flag = in.readBit();
            while (!flag)
                flag = in.readBit();
            int sign = (int) in.getLong(5);
            if (sign == 30) {
                if (in.getLong(64) == 0xFFFFFFFFFFFFFFFFL)
                    return false;
                else {
                    in.rollBack(64);
                    in.rollBack(5);
                }
            } else {
                in.rollBack(5);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        try {
            storedDelta = decodeZigZag64(in.getLong(FIRST_DELTA_BITS));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        storedValue = storedValue + storedDelta;
        System.out.println("Delta: " + storedDelta + " Val: " + storedValue);
        writeBuffer(dest, storedValue);
        return true;
    }

    private boolean writeBuffer(ByteBuffer dest, long storedValue) {
        if (dest.limit() - dest.position() < unitLength)
            return false;
        if (unitLength == 2)
            dest.putShort((short) storedValue);
        else if (unitLength == 4)
            dest.putInt((int) storedValue);
        else if (unitLength == 8)
            dest.putLong(storedValue);
        return true;
    }

    private boolean nextValue() {
        try {
            int readInstruction = in.nextClearBit(6);
            long deltaDelta;

            switch (readInstruction) {
                case 0x00:
                    storedValue += storedDelta;
                    return writeBuffer(dest, storedValue);
                case 0x02:
                    deltaDelta = in.getLong(7);
                    break;
                case 0x06:
                    deltaDelta = in.getLong(9);
                    break;
                case 0x0e:
                    deltaDelta = in.getLong(16);
                    break;
                case 0x1e:
                    deltaDelta = in.getLong(32);
                    break;
                case 0x3e:
                    deltaDelta = in.getLong(64);
                    // For storage save.. if this is the last available word, check if remaining bits are all 1
                    if (deltaDelta == 0xFFFFFFFFFFFFFFFFL) {
                        return false;
                    }
                    break;
                case 0x3f:
                    return false;
                default:
                    throw new RuntimeException("Fail to decompress value at position: " + in.getPosition() / 8 + " instruction: " + readInstruction);
            }
            deltaDelta++;
            deltaDelta = decodeZigZag64(deltaDelta);
            storedDelta += deltaDelta;
            storedValue += storedDelta;
            return writeBuffer(dest, storedValue);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
