package com.xxdb.compression;

public class DecoderFactory {

    private DecoderFactory() {}

    public static Decoder createLZ4Decoder() {
        return new LZ4Decoder();
    }

    public static Decoder createDeltaOfDeltaDecoder() {
        return new DeltaOfDeltaDecoder();
    }
}
