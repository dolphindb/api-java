package com.xxdb.compression;

public class BitInput {

    public final static long[] MASK_ARRAY;
    public final static long[] BIT_SET_MASK;

    static {
        MASK_ARRAY = new long[64];
        long mask = 1;
        long value = 0;
        for (int i = 0; i < MASK_ARRAY.length; i++) {
            value = value | mask;
            mask = mask << 1;

            MASK_ARRAY[i] = value;
        }

        BIT_SET_MASK = new long[64];
        for (int i = 0; i < BIT_SET_MASK.length; i++) {
            BIT_SET_MASK[i] = (1L << i);
        }
    }

    private final long[] longArray;
    private long lB;
    private int position = 0;
    private int bitsLeft = 0;

    public BitInput(long[] array) {
        this.longArray = array;
        flipByte();
    }

    public boolean readBit() {
        boolean bit = (lB & BIT_SET_MASK[bitsLeft - 1]) != 0;
        bitsLeft--;
        checkAndFlipByte();
        return bit;
    }

    private void flipByte() {
        lB = longArray[position++];
        bitsLeft = Long.SIZE;
    }

    private void checkAndFlipByte() {
        if (bitsLeft == 0) {
            flipByte();
        }
    }

    public int getInt() {
        return (int) getLong(32);
    }

    public long getLong(int bits) {
        long value;
        if (bits <= bitsLeft) {
            // We can read from this word only
            // Shift to correct position and take only n least significant bits
            value = (lB >>> (bitsLeft - bits)) & MASK_ARRAY[bits - 1];
            bitsLeft -= bits; // We ate n bits from it
            checkAndFlipByte();
        } else {
            // This word and next one, no more (max bits is 64)
            value = lB & MASK_ARRAY[bitsLeft - 1]; // Read what's left first
            bits -= bitsLeft;
            flipByte(); // We need the next one
            value <<= bits; // Give n bits of space to value
            value |= (lB >>> (bitsLeft - bits));
            bitsLeft -= bits;
        }
        return value;
    }

    public int nextClearBit(int maxBits) {
        int val = 0x00;

        for (int i = 0; i < maxBits; i++) {
            val <<= 1;
            boolean bit = readBit();

            if (bit) {
                val |= 0x01;
            } else {
                break;
            }
        }
        return val;
    }

    public int getPosition() {
        return position;
    }

}

