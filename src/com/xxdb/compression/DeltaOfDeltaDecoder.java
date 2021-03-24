package com.xxdb.compression;

public class DeltaOfDeltaDecoder {
    private long storedValue = 0;
    private long storedDelta = 0;

    private long blockValue = 0;
    private boolean endOfStream = false;
    private boolean isHeader = true;
    private boolean isFirst = true;

    public final static int FIRST_DELTA_BITS = Long.BYTES * 8;

    private final BitInput in;

    public DeltaOfDeltaDecoder(BitInput input) {
        in = input;
    }

    private void readHeader() {
        long header = in.getLong(64);
        blockValue = decodeZigZag64(header);
        storedValue = blockValue;
    }

    /**
     * Returns the next pair in the time series, if available.
     *
     * @return Pair if there's next value, null if series is done.
     */
    public long readValue() {
        if (isHeader) {
            readHeader();
            isHeader = false;
        } else
            next();
        if(endOfStream) {
            return -1111111;
        }
        return storedValue;
    }

    private void next() {
        // TODO I could implement a non-streaming solution also.. is there ever a need for streaming solution?

        if(isFirst) {
            first();
            isFirst = false;
            return;
        }

        nextValue();
    }

    private void first() {
        // First item to read
        storedDelta = decodeZigZag64(in.getLong(FIRST_DELTA_BITS));
        if(storedDelta == 0XFFFFFFFFFFFFFFFFL) {
            endOfStream = true;
            return;
        }
        storedValue = storedValue + storedDelta;
    }

    private void nextValue() {
        int readInstruction = in.nextClearBit(6);
        long deltaDelta;

        switch(readInstruction) {
            case 0x00:
                storedValue += storedDelta;
                return;
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
                    // End of stream
                    endOfStream = true;
                    return;
                }
                break;
            default:
                System.out.println("====================1====================");
                endOfStream = true;
                return;
        }

        deltaDelta++;
        deltaDelta = decodeZigZag64(deltaDelta);
        storedDelta = storedDelta + deltaDelta;

        storedValue += storedDelta;
    }

    // START: From protobuf

    /**
     * Decode a ZigZag-encoded 32-bit value. ZigZag encodes signed integers into values that can be
     * efficiently encoded with varint. (Otherwise, negative values must be sign-extended to 64 bits
     * to be varint encoded, thus always taking 10 bytes on the wire.)
     *
     * @param n An unsigned 32-bit integer, stored in a signed int because Java has no explicit
     *     unsigned support.
     * @return A signed 32-bit integer.
     */
    public static long decodeZigZag64(final long n) {
        return (n >>> 1) ^ -(n & 1);
    }
}
