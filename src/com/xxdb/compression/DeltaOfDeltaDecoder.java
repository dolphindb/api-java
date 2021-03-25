package com.xxdb.compression;

import com.xxdb.io.AbstractExtendedDataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DeltaOfDeltaDecoder {
    int unitLength;
    int dataCount = 0;
    int dataSize;

    private long storedValue = 0;
    private long storedDelta = 0;

    public final int FIRST_DELTA_BITS;

    private BitInput in;
    private final ByteBuffer dest;

    public DeltaOfDeltaDecoder(AbstractExtendedDataInputStream in, ByteBuffer dest, int srcSize, int dataSize, int unitLength) {
        FIRST_DELTA_BITS = unitLength * 8;
        this.dest = dest;
        this.unitLength = unitLength;
        this.dataSize = dataSize;
        int blockSize = 0;
        int count = 0;
        while (count < srcSize && dataCount < dataSize) {
            try {
                blockSize = in.readInt();
                count+=Integer.BYTES;
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (blockSize == 0) break;
            blockSize = Math.min(blockSize, srcSize - count);
            long[] bytes = new long[blockSize/Long.BYTES];
            try {
                for (int i = 0; i < bytes.length; i++) {
                    bytes[i] = in.readLong();
                }
                if(!decompress(bytes, blockSize))
                    break;
            } catch (Exception e) {
                e.printStackTrace();
            }
            count+=blockSize;
        }
        dest.flip();
    }

    public byte[] decompress() {
        return dest.array();
    }

    private boolean decompress(long[] bytes, int blockSize) {
        in = new BitInput(bytes);
        boolean flag = in.readBit();
        while (!flag) {
            flag = in.readBit();
            writeBuffer(dest, 0L);
            if (in.getPosition() > bytes.length - 3 || dataCount >= dataSize) return true;
        }
        if (!readHeader() || !first())
            return false;
        while (in.getPosition()/8 < blockSize) {
            if (!nextValue()) break;
        }
        return true;
    }

    private boolean readHeader() {
        try {
            storedValue = decodeZigZag64(in.getLong(unitLength * 8));
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
        writeBuffer(dest, storedValue);
        return true;
    }

    private void writeBuffer(ByteBuffer dest, long storedValue) {
        if (unitLength == 2)
            dest.putShort((short) storedValue);
        else if (unitLength == 4)
            dest.putInt((int) storedValue);
        else if (unitLength == 8)
            dest.putLong(storedValue);
        dataCount++;
    }


    private boolean nextValue() {
        try {
            int readInstruction = in.nextClearBit(6);
            long deltaDelta;

            switch(readInstruction) {
                case 0x00:
                    storedValue += storedDelta;
                    writeBuffer(dest, storedValue);
                    return true;
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
                    throw new RuntimeException("Fail to decompress value at position: " + in.getPosition()/8 + " instruction: " + readInstruction);
            }
            deltaDelta++;
            deltaDelta = decodeZigZag64(deltaDelta);
            storedDelta += deltaDelta;
            storedValue += storedDelta;
            writeBuffer(dest, storedValue);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            writeBuffer(dest, 0);
            return false;
        }
    }

    public static long decodeZigZag64(final long n) {
        return (n >>> 1) ^ -(n & 1);
    }
}
