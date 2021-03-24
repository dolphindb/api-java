package com.xxdb.compression;

import com.xxdb.io.AbstractExtendedDataInputStream;

import java.io.IOException;

public class BitInput {
    private AbstractExtendedDataInputStream is;
    private byte b;
    private int bitsLeft = 0;
    private int position = 0;

    public BitInput(AbstractExtendedDataInputStream is) {
        this.is = is;
        try {
            flipByte();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean readBit() {
        boolean bit = ((b >> (bitsLeft - 1)) & 1) == 1;
        bitsLeft--;
        try {
            flipByte();
        } catch (IOException e) {
            e.printStackTrace();
        }
        position++;
        return bit;
    }

    public long getLong(int bits) {
        long value = 0;
        while(bits > 0) {
            if(bits > bitsLeft || bits == Byte.SIZE) {
                // Take only the bitsLeft "least significant" bits
                byte d = (byte) (b & ((1<<bitsLeft) - 1));
                value = (value << bitsLeft) + (d & 0xFF);
                bits -= bitsLeft;
                bitsLeft = 0;
            } else {
                // Shift to correct position and take only least significant bits
                byte d = (byte) ((b >>> (bitsLeft - bits)) & ((1<<bits) - 1));
                value = (value << bits) + (d & 0xFF);
                bitsLeft -= bits;
                bits = 0;
            }
            try {
                flipByte();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        position+=bits;
        return value;
    }


    public int nextClearBit(int maxBits) {
        int val = 0x00;

        for(int i = 0; i < maxBits; i++) {
            val <<= 1;
            boolean bit = readBit();

            if(bit) {
                val |= 0x01;
            } else {
                break;
            }
        }
        return val;
    }

    private void flipByte() throws IOException {
        if (bitsLeft == 0) {
            b = is.readByte();
            bitsLeft = Byte.SIZE;
        }
    }

    public int getPosition() {
        return position;
    }
}

