package com.xxdb.compression;

public class EncoderFactory {
    private EncoderFactory() {}

    public static Encoder get(int method){
        if(method == 1)
            return new LZ4Encoder();
        else if(method == 2)
            return new DeltaOfDeltaEncoder();
        else
            throw new RuntimeException("Invalid compression method " + method);
    }
}
