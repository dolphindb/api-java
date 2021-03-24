package com.xxdb.compression;

import com.xxdb.io.AbstractExtendedDataInputStream;
import com.xxdb.io.ExtendedDataInput;

import java.nio.ByteBuffer;

public class DeltaOfDeltaDecoder {
    private long storedValue = 0;
    private long storedDelta = 0;

    private long blockValue = 0;
    private boolean endOfStream = false;
    private boolean isHeader = true;
    private boolean isFirst = true;

    public final static int FIRST_DELTA_BITS = Long.BYTES * 8;

    private final BitInput in;
    private ByteBuffer dest;

    public DeltaOfDeltaDecoder(AbstractExtendedDataInputStream in, int blockSize, int dataSize) {
        this.in = new BitInput(in);
        this.dest = ByteBuffer.allocate(dataSize * Long.BYTES + 20);
        int count = 0;
        while ((count + 1) * blockSize <= dataSize) {
            if (!decompress(blockSize)) throw new RuntimeException("Cannot decompress data: stop at" + this.in.getPosition());
            count++;
        }
        if (dataSize - count * blockSize > 0)
            if (!decompress(dataSize - count * blockSize))
                throw new RuntimeException("Cannot decompress data: stop at" + this.in.getPosition());
    }

    private boolean decompress(int blockSize) {
        int start = in.getPosition();
        if (!readHeader() || !first())
            return false;
        while (in.getPosition() < blockSize) {
            if (!nextValue()) break;
        }
        return true;
    }


    private boolean readHeader() {
        blockValue = decodeZigZag64(in.getLong(64));
        storedValue = blockValue;
        dest.putLong(storedValue);
        return true;
    }

    private boolean first() {
        storedDelta = decodeZigZag64(in.getLong(FIRST_DELTA_BITS));
        if(storedDelta == 0XFFFFFFFFFFFFFFFFL) {
            return false;
        }
        storedValue = storedValue + storedDelta;
        dest.putLong(storedValue);
        return true;
    }

    private boolean nextValue() {
        int readInstruction = in.nextClearBit(6);
        long deltaDelta;

        switch(readInstruction) {
            case 0x00:
                storedValue += storedDelta;
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
            default:
                throw new RuntimeException("Fail to decompress value at position: " + in.getPosition());
        }

        deltaDelta++; //FIXME: why deltaDelta++?
        deltaDelta = decodeZigZag64(deltaDelta);
        storedDelta += deltaDelta;
        storedValue += storedDelta;
        return true;
    }

    public static long decodeZigZag64(final long n) {
        return (n >>> 1) ^ -(n & 1);
    }
}
